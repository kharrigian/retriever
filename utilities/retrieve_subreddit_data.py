
####################
### Imports
####################

## Standard Libary
import sys
import os
import sys
import json
import gzip
import argparse
from time import sleep

## External
import pandas as pd
from tqdm import tqdm

## Local
from retriever import Reddit
from retriever.util.helpers import chunks
from retriever.util.logging import get_logger

####################
### Globals
####################

## Data Parameters
OUTDIR = "./data/subreddits/"

## Logger
LOGGER = get_logger()

## Filter Columns (To Reduce Request Load)
SUBMISSION_COLS = [
    "author",
    "author_fullname",
    "num_comments",
    "created_utc",
    "id",
    "permalink",
    "selftext",
    "title",
    "subreddit",
    "subreddit_id",
]

####################
### Functions
####################

def parse_arguments():
    """

    Parse command-line to identify configuration filepath.
    Args:
        None
    
    Returns:
        args (argparse Object): Command-line argument holder.
    """
    ## Initialize Parser Object
    parser = argparse.ArgumentParser(description="Query Reddit Submissions and Comments")
    ## Generic Arguments
    parser.add_argument("subreddit", type=str, help="Name of the subreddit to find submissions and comments for")
    parser.add_argument("--start_date", type=str, default="2019-01-01", help="Start date for data")
    parser.add_argument("--end_date", type=str, default="2020-08-01", help="End date for data")
    parser.add_argument("--query_freq", type=str, default="7D", help="How to break up the submission query")
    parser.add_argument("--min_comments", type=int, default=0, help="Filtering criteria for querying comments based on submissions")
    parser.add_argument("--use_praw", action="store_true", default=False, help="Retrieve Official API data objects (at expense of query time) instead of Pushshift.io data")
    parser.add_argument("--chunksize", type=int, default=50, help="Number of submissions to query comments from simultaneously")
    parser.add_argument("--sample_percent", type=float, default=1, help="Submission sample percent (0, 1]")
    parser.add_argument("--random_state", type=int, default=42, help="Sample seed for any submission sampling")
    ## Parse Arguments
    args = parser.parse_args()
    return args

def create_dir(directory):
    """

    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    
def get_date_range(start_date,
                   end_date,
                   query_freq):
    """

    """
    ## Query Date Range
    DATE_RANGE = list(pd.date_range(start_date, end_date, freq=query_freq))
    if pd.to_datetime(start_date) < DATE_RANGE[0]:
        DATE_RANGE = [pd.to_datetime(start_date)] + DATE_RANGE
    if pd.to_datetime(end_date) > DATE_RANGE[-1]:
        DATE_RANGE = DATE_RANGE + [pd.to_datetime(end_date)]
    DATE_RANGE = [d.date().isoformat() for d in DATE_RANGE]
    return DATE_RANGE

def main():
    """

    """
    ## Parse Arguments
    args = parse_arguments()
    ## Initialize Reddit API Wrapper
    reddit = Reddit(args.use_praw)
    ## Create Output Directory
    _ = create_dir(OUTDIR)
    ## Get Date Range
    DATE_RANGE = get_date_range(args.start_date,
                                args.end_date,
                                args.query_freq)
    ## Create Output Directory
    LOGGER.info(f"\nStarting Query for r/{args.subreddit}")
    SUBREDDIT_OUTDIR = f"{OUTDIR}{args.subreddit}/"
    SUBREDDIT_SUBMISSION_OUTDIR = f"{SUBREDDIT_OUTDIR}submissions/"
    _ = create_dir(SUBREDDIT_OUTDIR)
    _ = create_dir(SUBREDDIT_SUBMISSION_OUTDIR)    
    ## Identify Submission Data
    LOGGER.info("Pulling Submissions")
    submission_files = []
    submission_counts = []
    for dstart, dstop in tqdm(list(zip(DATE_RANGE[:-1],DATE_RANGE[1:])), desc="Date Range", file=sys.stdout):
        submission_file = f"{SUBREDDIT_SUBMISSION_OUTDIR}{dstart}_{dstop}.json.gz"
        submission_files.append(submission_file)
        if os.path.exists(submission_file):
            continue
        ## Query Submissions
        subreddit_submissions = reddit.retrieve_subreddit_submissions(args.subreddit,
                                                                      start_date=dstart,
                                                                      end_date=dstop,
                                                                      limit=None,
                                                                      cols=SUBMISSION_COLS)
        submission_json = []
        if subreddit_submissions is not None and len(subreddit_submissions) > 0:
            submission_counts.append(len(subreddit_submissions))
            for r, row in subreddit_submissions.iterrows():
                submission_json.append(json.loads(row.to_json()))
        with gzip.open(submission_file,"wt") as the_file:
            json.dump(submission_json, the_file)
    LOGGER.info("Found {:,d} submissions".format(sum(submission_counts)))
    ## Pull Comments
    LOGGER.info("Pulling Comments")
    SUBREDDIT_COMMENTS_DIR = f"{SUBREDDIT_OUTDIR}comments/"
    _ = create_dir(SUBREDDIT_COMMENTS_DIR)
    for sub_file in tqdm(submission_files, desc="Date Range", position=0, leave=False, file=sys.stdout):
        subreddit_submissions = pd.read_json(sub_file)
        if len(subreddit_submissions) == 0:
            continue
        if args.sample_percent < 1:
            subreddit_submissions = subreddit_submissions.sample(frac=args.sample_percent,
                                                                    random_state=args.random_state,
                                                                    replace=False).reset_index(drop=True).copy()
        link_ids = subreddit_submissions.loc[subreddit_submissions["num_comments"] > args.min_comments]["id"].tolist() 
        link_ids = [l for l in link_ids if not os.path.exists(f"{SUBREDDIT_COMMENTS_DIR}{l}.json.gz")]
        if len(link_ids) == 0:
            continue
        link_id_chunks = list(chunks(link_ids, args.chunksize))
        for link_id_chunk in tqdm(link_id_chunks, desc="Submission Chunks", position=1, leave=False, file=sys.stdout):
            link_df = reddit.retrieve_submission_comments(link_id_chunk)
            for link_id in link_id_chunk:
                link_json = []
                link_file = f"{SUBREDDIT_COMMENTS_DIR}{link_id}.json.gz"
                if link_df is None or len(link_df) == 0:
                    pass
                else:
                    link_id_df = link_df.loc[link_df["link_id"]==f"t3_{link_id}"]
                    if link_id_df is not None and len(link_id_df) > 0:
                        for r, row in link_id_df.iterrows():
                            link_json.append(json.loads(row.to_json()))
                with gzip.open(link_file,"wt") as the_file:
                    json.dump(link_json, the_file)
    LOGGER.info("Script complete.")

####################
### Execute
####################

if __name__ == "__main__":
    main()
