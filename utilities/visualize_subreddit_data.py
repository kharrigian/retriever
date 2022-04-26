
"""
Small utility to look at statistics for subreddit data acquired by retrieve_subreddit_data.py
"""

####################
### Imports
####################

## Standard Library
import os
import argparse
from glob import glob
from datetime import datetime
from multiprocessing import Pool

## External Libraries
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

## Package
from retriever.util.logging import get_logger

####################
### Globals
####################

## Logging
LOGGER = get_logger()

####################
### Functions
####################

def main():
    """
    
    """
    ## Command Line
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("subreddit", type=str, help="Name of the subreddit stored on disk.")
    _ = parser.add_argument("--data_dir", type=str, default="./data/subreddits/", help="Where the subreddit data is stored.")
    _ = parser.add_argument("--smoothing", type=int, default=None, help="Number of days to aggregate in visualization.")
    _ = parser.add_argument("--output_dir", type=str, default=None, help="Where to save the visualization. Default is None (no caching).")
    _ = parser.add_argument("--jobs", type=int, default=1, help="Number of processes to use for loading data.")
    args = parser.parse_args()
    ## Output Directory
    if args.output_dir is not None and not os.path.exists(args.output_dir):
        _ = os.makedirs(args.output_dir)
    ## Filenames
    submission_files = glob("{}/{}/submissions/*.json.gz".format(args.data_dir, args.subreddit))
    comment_files = glob("{}/{}/comments/*.json.gz".format(args.data_dir, args.subreddit))
    for f, ftype in zip([submission_files, comment_files],["Submissions","Comments"]):
        if len(f) == 0:
            LOGGER.warning("Warning: No {} Found.".format(ftype))
    ## Load Data
    submissions_df, comments_df = None, None
    with Pool(args.jobs) as mp:
        if len(submission_files) > 0:
            submissions_df = pd.concat(list(tqdm(mp.imap_unordered(pd.read_json, submission_files), desc="[Loading Submissions]", total=len(submission_files)))).reset_index(drop=True)
        if len(comment_files) > 0:
            comments_df = pd.concat(list(tqdm(mp.imap_unordered(pd.read_json, comment_files), desc="[Loading Comments]", total=len(comment_files)))).reset_index(drop=True)
    if submissions_df is None and comments_df is None:
        LOGGER.warning("Warning: No data to plot. Exiting.")
        return None
    ## Summary Statistics
    timestamps = {}
    data_agg = {}
    for f, ftype, ftext in zip([submissions_df, comments_df],["Submissions","Comments"],[["title","selftext"],["body"]]):
        ## Check
        if f is None:
            continue
        ## Get Filters
        text_mask = ~ (f[ftext].fillna("").apply(lambda i: " ".join(i), axis=1).map(lambda i: i.strip() == "[deleted]" or i.strip() == "[removed]" or len(i.strip()) == 0))
        author_mask = ~ (f["author"].map(lambda i: i == "[deleted]" or i == "[removed]" or i == "AutoModerator"))
        ## Apply Filter
        ffilt = f.loc[pd.concat([text_mask, author_mask],axis=1).all(axis=1)].reset_index(drop=True)
        ## Statistics
        n_author_unique = len(ffilt["author"].unique())
        n_total = ffilt.shape[0]
        LOGGER.warning("{} Statistics: {:,d} Unique Authors, {:,d} Total Posts".format(ftype, n_author_unique, n_total))
        ## Datetime Formatting
        ffilt["date"] = ffilt["created_utc"].map(datetime.fromtimestamp).map(lambda i: i.date())
        timestamps[ftype] = ffilt["date"]
        ## Aggregate Stats
        ffilt_agg = ffilt[["date","author"]+ftext].copy()
        ffilt_agg = ffilt_agg.groupby(["date"]).agg({"author":[lambda x: len(set(x)), len]})
        ffilt_agg.columns = ["n_unique_users","n_posts"]
        data_agg[ftype] = ffilt_agg
    ## Format Aggregated Data
    data_agg = pd.concat(data_agg)
    data_agg = pd.merge(data_agg.loc["Submissions"], data_agg.loc["Comments"], left_index=True, right_index=True, how="outer", suffixes=("_submission","_comment"))
    data_agg = data_agg.reindex(pd.date_range(data_agg.index.min(), data_agg.index.max())).fillna(0).astype(int)
    if args.output_dir is not None:
        _ = data_agg.to_csv(f"{args.output_dir}/{args.subreddit}.counts.csv",index=True)
    ## Distribution over Time
    fig, ax = plt.subplots(len(timestamps), 1, figsize=(12, 5), sharex=True)
    for j, (ftype, ftimestamp) in enumerate(timestamps.items()):
        jax = ax if len(timestamps) == 1 else ax[j]
        fvc = ftimestamp.value_counts().sort_index()
        fvc = fvc.reindex(pd.date_range(fvc.index.min(), fvc.index.max())).fillna(0).astype(int)
        if args.smoothing is not None:
            fvc = fvc.rolling(args.smoothing, closed="left").sum().dropna()
        fvc.plot(ax=jax, color="navy", alpha=0.5, label="Posts Per Day" if args.smoothing is None else "{}-day Total".format(args.smoothing))
        jax.set_title(ftype, loc="left", fontweight="bold")
        jax.spines["right"].set_visible(False)
        jax.spines["top"].set_visible(False)
        jax.tick_params(labelsize=12)
        jax.legend(loc="upper left", fontsize=12)
    jax.set_xlabel("Date", fontweight="bold", fontsize=12)
    fig.text(0.025, 0.5, "# Posts", rotation=90, ha="center", va="center", fontweight="bold", fontsize=12)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.subplots_adjust(left=0.075)
    if args.output_dir is not None:
        fig.savefig(f"{args.output_dir}/{args.subreddit}.png", dpi=300)
    plt.show()
    
####################
### Execution
####################

if __name__ == "__main__":
    _ = main()