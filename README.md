# Retriever

A wrapper for API wrappers. Currently supports Reddit (via `praw` and `psaw`).

## Status

**Warning**: As of July 7th, this package is not functional due to changes in Reddit's policy regarding third-party APIs. Please consult the [Pushshift subreddit](https://www.reddit.com/r/pushshift) to check on the status of the backend. I have opted to keep the code public in case anything changes in the future.

## Contact

Questions or concerns? Feel free to reach out to Keith Harrigian at <kharrigian@jhu.edu>. If you encounter any issues with the package, please consider submitting an issue on [Github](https://github.com/kharrigian/retriever).

## Credentials

Prior to installing the package, you should update the `retriever/config.json` file with official API credentials for the platforms you plan to query data from. For Reddit, credentials are not explicitly necessary if you only plan to use functionality associated with the Pushshift.io API. However, if you are interested in querying updated comment scores or any subreddit metadata, you will need to provide the library with credentials. The official API is also required to do a "fallback" search for comments in the case that Pushshift Shards are not working and/or other missing-data challenges emerge.

## Installation

The package requires Python 3.7+. If you do not plan on updating credentials constantly, you can install the package to your system library using `pip install .`. Otherwise, we recommend installing locally with `pip install -e .`.

## Testing

To ensure the `retriever` has been properly installed, you may run the test suite using the following code.

```
pytest tests/ -Wignore -v
```

## Usage

To interact with the API, simply import your desired platform wrapper i.e.

```
from retriever import Reddit
wrapper = Reddit()
```

Docstrings are the best resource currently for learning about the functionalities of the package. That said, we provide example usage in `utilities/retrieve_subreddit_data.py`. This script also serves as a useful resource for acquiring all comments and submissions for a subreddit based on some set of constraints. Explore some of the functionaliy using by running

```
python utilities/retrieve_subreddit_data.py --help
```
