
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
from datetime import datetime, timedelta

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
    parser.add_argument("--start_date", type=str, default=None, help="Start date for data")
    parser.add_argument("--end_date", type=str, default=None, help="End date for data")
    parser.add_argument("--query_freq", type=str, default="7D", help="How to break up the submission query")
    parser.add_argument("--min_comments", type=int, default=0, help="Filtering criteria for querying comments based on submissions")
    parser.add_argument("--use_praw", action="store_true", default=False, help="Retrieve Official API data objects (at expense of query time) instead of Pushshift.io data")
    parser.add_argument("--allow_praw", action="store_true", default=False, help="Allow use of PRAW (if available) when PSAW doesn't return any data.")
    parser.add_argument("--chunksize", type=int, default=50, help="Number of submissions to query comments from simultaneously")
    parser.add_argument("--sample_percent", type=float, default=1, help="Submission sample percent (0, 1]")
    parser.add_argument("--random_state", type=int, default=42, help="Sample seed for any submission sampling")
    parser.add_argument("--cache_empty", action="store_true", default=False, help="If included, will store empty comment files to skip next time around.")
    parser.add_argument("--comment_max_range", type=int, default=None, help="Number of days after a submission was posted to restrict comment collection. Default is None.")
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
    ## Update Defaults
    if start_date is None:
        start_date = "2015-01-01"
    if end_date is None:
        end_date = datetime.now().date().isoformat()
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
    reddit = Reddit(init_praw=args.use_praw, allow_praw=args.allow_praw)
    ## Create Output Directory
    _ = create_dir(OUTDIR)
    ## Get Date Range
    DATE_RANGE = get_date_range(args.start_date,
                                args.end_date,
                                args.query_freq)
    ## Create Output Directory
    LOGGER.warning(f"\nStarting Query for r/{args.subreddit}")
    SUBREDDIT_OUTDIR = f"{OUTDIR}{args.subreddit}/"
    SUBREDDIT_SUBMISSION_OUTDIR = f"{SUBREDDIT_OUTDIR}submissions/"
    _ = create_dir(SUBREDDIT_OUTDIR)
    _ = create_dir(SUBREDDIT_SUBMISSION_OUTDIR)    
    ## Identify Submission Data
    LOGGER.warning("Pulling Submissions")
    submission_files = []
    submission_counts = []
    for dstart, dstop in tqdm(list(zip(DATE_RANGE[:-1],DATE_RANGE[1:])), desc="Date Range", file=sys.stdout):
        submission_file = f"{SUBREDDIT_SUBMISSION_OUTDIR}{dstart}_{dstop}.json.gz"
        submission_files.append(submission_file)
        if os.path.exists(submission_file):
            ## Cache Number of Submissions
            with gzip.open(submission_file,"r") as the_file:
                submission_counts.append(len(json.load(the_file)))
            ## Move Forward
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
    LOGGER.warning("Found {:,d} submissions".format(sum(submission_counts)))
    ## Pull Comments
    LOGGER.warning("Pulling Comments")
    SUBREDDIT_COMMENTS_DIR = f"{SUBREDDIT_OUTDIR}comments/"
    _ = create_dir(SUBREDDIT_COMMENTS_DIR)
    q_totals = []
    for sub_file in tqdm(submission_files, desc="Date Range", position=0, leave=False, file=sys.stdout):
        ## Load Submissions
        subreddit_submissions = pd.read_json(sub_file)
        ## Start/End Search Range
        sub_file_start_date = os.path.basename(sub_file).split("_")[0]
        if args.comment_max_range is None:
            sub_file_end_date = None
        else:
            sub_file_end_date = pd.to_datetime(sub_file_start_date).date() + timedelta(args.comment_max_range)
            sub_file_end_date = sub_file_end_date.isoformat()
        ## Check Length
        if len(subreddit_submissions) == 0:
            continue
        ## Downsampling
        if args.sample_percent < 1:
            subreddit_submissions = subreddit_submissions.sample(frac=args.sample_percent,
                                                                 random_state=args.random_state,
                                                                 replace=False).reset_index(drop=True).copy()
        ## Filtering (Comments and Existence)
        link_ids = subreddit_submissions.loc[subreddit_submissions["num_comments"] >= args.min_comments]["id"].tolist() 
        link_ids = [l for l in link_ids if not os.path.exists(f"{SUBREDDIT_COMMENTS_DIR}{l}.json.gz")]
        ## See if Done
        if len(link_ids) == 0:
            continue
        ## Group into Query Chunks
        link_id_chunks = list(chunks(link_ids, args.chunksize))
        ## Iterate Through Chunks
        n_total, n_empty = 0, 0
        for link_id_chunk in tqdm(link_id_chunks, desc="Submission Chunks", position=1, leave=False, file=sys.stdout):
            ## Update Total
            n_total += len(link_id_chunk)
            ## Query Comments
            link_df = reddit.retrieve_submission_comments(link_id_chunk,
                                                          start_date=sub_file_start_date,
                                                          end_date=sub_file_end_date)
            ## Cache Comments by Thread
            for link_id in link_id_chunk:
                ## Initialize Thread Cache
                link_json = []
                if link_df is None or len(link_df) == 0:
                    if args.cache_empty:
                        pass
                    else:
                        n_empty += 1
                        continue
                else:
                    ## Look for Thread
                    link_id_df = link_df.loc[(link_df["link_id"]==f"t3_{link_id}")|(link_df["link_id"]==link_id)]
                    ## Format as JSON
                    if link_id_df is not None and len(link_id_df) > 0:
                        for _, row in link_id_df.iterrows():
                            link_json.append(json.loads(row.to_json()))
                ## Update Total
                if len(link_json) == 0:
                    n_empty += 1
                ## Cache Thread
                if len(link_json) > 0 or args.cache_empty:
                    link_file = f"{SUBREDDIT_COMMENTS_DIR}{link_id}.json.gz"
                    with gzip.open(link_file,"wt") as the_file:
                        json.dump(link_json, the_file)
        ## Cache Totals
        q_totals.append((n_total, n_empty))
    LOGGER.warning("Script complete.")

####################
### Execute
####################

if __name__ == "__main__":
    main()
