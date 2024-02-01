version 1.0

workflow BufferregionShards {
  input {
    File vcf
    File vcf_idx
    Int region_span = 1000000
    Int buffer = 1000
  }

  call SplitRegions {
    input:
      vcf = vcf,
      region_span = region_span,
      variant_buffer = buffer
  }

  call ShardVcfByRegion {
    vcf = vcf,
    vcf_idx = vcf_idx,
    scatter_regions = SplitRegions.regions
  }

  output {
    File vcf_shards = ShardVcfByRegion.vcf_shards
    File vcf_shard_idxs = ShardVcfByREgion.vcf_shard_idxs
  }
}

task SplitRegions {
  input {
    File vcf
    Int region_span = 1000000
    Int variant_buffer = 100
  }

  Int disk_gb = ceil(1.3 * size(vcf, "GB"))

  command <<<
    python3 - <<'__script__'
      import math
      from collections import deque
      import numpy as np

      in_vcf = ~{vcf}
      max_span = ~{region_span}
      buffer = ~{variant_buffer}

      xrms = np.loadtxt(in_vcf, usecols=(0), dtype='U')
      pos = np.loadtxt(in_vcf, usecols=(1), dtype=np.int32)

      # unfortunately numpy sorts before returning the unique elements,
      #  so we have to unsort (i.e. re-sort the indices into order)
      Xorder = list(xrms[sorted(np.unique(xrms, return_index=True)[1])])

      L, U = np.clip(pos - buffer, 1, None), pos + buffer             # Lower, Upper buffers around each variant
      I = np.nonzero((L[1:] > U[:-1]) | (xrms[1:] != xrms[:-1]))[0]   # where buffer boundaries _dont_ overlap
      iA, iB = np.concatenate([[0], I+1]), np.concatenate([I, [-1]])  # lower, upper merged buffer indices
      X, A, B = xrms[iA], L[iA], U[iB]                                # merged buffers (i.e. subregions)
      S = (B-A).sum()                                                 # total subregion span
      N = math.ceil(S/max_span)                                       # number of regions
      T = math.ceil(S/N)                                              # target region length (for equal distribution)

      print(S, 'sized buffer to', T, 'length fragments')
      # deques pop from the end, so we have to load in backwards
      queue = deque(list(zip(X, A, B))[::-1])
      s, i, N = 0, 0, True
      with open(f"{in_vcf}.scatter_regions.txt", 'w') as out:
        while len(queue) > 0:
          x, a, b = queue.pop()   # deque supports O(1) resizing
          if N: xl, al = x, a     # if the region is new we need to start a new record
          elif x != xl:           # if the region didn't fill and the chromosome changes...  
            out.write(f'{xl}:{al}-{bl}\t{i}\n') # we need to record what we had...
            xl, al = x, a                       # and start a new record
          if b-a > (r := T-s):    # split a subregion if it overflows a region, r is what will fit
            m = a + r             # mark the new end point to fill the region; half open intervals so m is end and next start
            queue.append((x,m,b)) # the chromosome won't change next pass, but the region will. ...
            b = m                 #   while it's unlikely for this to not happen, it's not impossible
          s += b-a                # contribute subregion measure, half open intervals -> no +1
          if (N := s >= T):       # N signals a new region/this region filled
            out.write(f'{x}:{al}-{b}\t{i}\n')   # x == xl here, so al and b are colinear
            i += 1                # increment region count
            s = 0                 # reset region measure
          bl = b                  # remember where we left off in case the chromosome changes
        if not N: out.write(f'{x}:{al}-{b}\t{i}\n') # If the region didnt fill and there's no next subregion, we need to record what we have

    __script__
  >>>

  output {
    File regions = "~{vcf}.scatter_regions.txt"
  }

  runtime {
    cpu: 1
    memory: "3.75 GiB"
    disks: "local-disk " + disk_gb + " HDD"
    preemptible: 3
    docker: "python:latest"
  }

}

task ShardVcfByRegion {
  input {
    File vcf
    File vcf_idx
    File scatter_regions
    String bcftools_docker = "quay.io/biocontainers/bcftools:1.18--h8b25389_0"
  }

  Int disk_gb = ceil(3 * size(vcf, "GB"))

  command <<<
    set -eu -o pipefail

    # Strips either .vcf or .bcf, and .gz if present, appends sharded
    out_prefix=$(awk -v fname="root.bcf.gz" 'BEGIN {sub(/.[vb]cf(.gz)?/,"",fname); print fname".sharded"}');
    echo ${out_prefix} > out_prefix.txt

    # Make an empty shard in case the input VCF is totally empty
    bcftools view -h ~{vcf} | bgzip -c > "${out_prefix}.0.vcf.gz"

    bcftools +scatter \
      -O z3 -o . -p "${out_prefix}". \
      -S ~{scatter_regions} \
      ~{vcf}

    # Print all VCFs to stdout for logging purposes
    find ./ -name "*.vcf.gz"

    # Index all shards
    find ./ -name "${out_prefix}.*.vcf.gz" \
    | xargs -I {} tabix -p vcf -f {}
  >>>

  output {
    String out_prefix = read_string("out_prefix.txt")
    Array[File] vcf_shards = glob("~{out_prefix}.*.vcf.gz")
    Array[File] vcf_shard_idxs = glob("~{out_prefix}.*.vcf.gz.tbi")
  }

  runtime {
    cpu: 2
    memory: "3.75 GiB"
    disks: "local-disk " + disk_gb + 20 + " HDD"
    bootDiskSizeGb: 10
    docker: bcftools_docker
    preemptible: 3
    maxRetries: 1
  }
}