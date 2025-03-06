# Google Cloud Setup

## Application Default Credentials
To run these tools locally (and from non-cloud terminal environments), the google cloud storage python api and firecloud python api will need permission through an account with access to the resources you'd like to work on. Most likely this will be your google account. These are instructions to set up the credentials that cloud applications will use by default. These tools don't currently support providing credentials by any other method.

1. Install the [gcloud command line interface](https://cloud.google.com/sdk/docs/install#windows)
    - Note the instructions vary by OS
2. Open a terminal
    - If the installation did not run `gcloud init`, do so now
3. Run `gcloud auth application-default login` and follow the instructions.

## Python APIs
Beyond the standard library, these tools make use of
- numpy
- pandas
- firecloud (not in conda default channels)
- google-cloud-storage=2.19.0 (not in conda default channels)

All packages are available through pypi
```
pip install numpy pandas firecloud google-cloud-storage=2.19.0
```

A mixed installation can be done with conda
```
conda install numpy pandas
pip install firecloud google-cloud-storage=2.19.0
```