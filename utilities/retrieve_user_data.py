
"""
Collect comments and submissions from a user

Note: The only way to ensure comment/submission limits act appropriately is
to use a users_per_chunk size of 1. Otherwise, it's possible for the query to
be incomplete for some users, while overcomplete for others in the chunk.
"""

######################
### Imports
######################

## Standard Library
import os
import sys
import json
import gzip
import argparse
from glob import glob
from datetime import datetime
from multiprocessing import cpu_count, Pool
from collections import Counter

## External Libraries
import pandas as pd
from tqdm import tqdm

## Package
from retriever import Reddit
from retriever.util.logging import get_logger
from retriever.util.helpers import chunks, flatten

######################
### Globals
######################

## Logging
LOGGER = get_logger()

## Users to Ignore
IGNORE_SET = set([
    "AutoModerator",
    "[deleted]",
    "[removed]"
])

######################
### Functions
######################

def parse_command_line():
    """
    
    """
    ## Create Parser
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("inputs", type=str, nargs="+", help="Paths to .txt or .json.gz files")
    _ = parser.add_argument("--output_dir", default="./data/users/", type=str)
    _ = parser.add_argument("--get_comments", action="store_true", default=False)
    _ = parser.add_argument("--get_submissions", action="store_true", default=False)
    _ = parser.add_argument("--start_date", type=str, default=None)
    _ = parser.add_argument("--end_date", type=str, default=None)
    _ = parser.add_argument("--use_praw", action="store_true", default=False)
    _ = parser.add_argument("--max_comments", type=int, default=None)
    _ = parser.add_argument("--max_submissions", type=int, default=None)
    _ = parser.add_argument("--ignore_existing", action="store_true", default=False)
    _ = parser.add_argument("--users_per_chunk", type=int, default=10)
    args = parser.parse_args()
    ## Check/Initialize
    if not args.get_comments and not args.get_submissions:
        raise ValueError("Must specify at least one of --get_comments or --get_submissions")
    if not os.path.exists(args.output_dir):
        LOGGER.warning(f"Creating output directory at: '{args.output_dir}'")
        _ = os.makedirs(args.output_dir)
    return args

def load_txt_data(filename):
    """
    
    """
    ## Load File 
    data = []
    with open(filename,"r") as the_file:
        for line in the_file:
            data.append(line.strip())
    return data

def load_json_data(filename):
    """
    
    """
    ## Load File
    data = []
    with gzip.open(filename,"r") as the_file:
        for line in the_file:
            ## Format
            line_data = json.loads(line)
            ## Check Type
            if not isinstance(line_data, list) and isinstance(line_data, dict):
                line_data = [line_data]
            elif isinstance(line_data, list) and not all(isinstance(ld, dict) for ld in line_data):
                raise TypeError("Expected list of dictionaries.")
            elif isinstance(line_data, list) and all(isinstance(ld, dict) for ld in line_data):
                pass
            else:
                raise TypeError(f"Data type not understood for file: '{filename}'")
            ## Update Username Cache
            data.extend(line_data)
    return data

def _enumerate_users(filename):
    """
    
    """
    ## Case 1: .txt file (New-line Delimited)
    if filename.endswith(".txt"):
        return load_txt_data(filename)
    ## Case 2: .json.gz file
    elif filename.endswith(".json.gz"):
        return [j.get("author") for j in load_json_data(filename)]
    ## Other Cases: Will add support as they arise
    else:
        raise TypeError(f"File type not understood: '{filename}'")

def enumerate_users(inputs):
    """
    
    """
    ## Isolate Filenames
    input_filenames = []
    for i in inputs:
        if "*" in i:
            ifiles = glob(i)
            if len(ifiles) == 0:
                LOGGER.warning(f"Warning: No files found from input '{i}'")
        else:
            ifiles = [i]
        input_filenames.extend(ifiles)
    ## Ensure Filename Existence
    input_filenames = list(filter(lambda f: os.path.exists(f), input_filenames))
    ## Load Users
    usernames = Counter()
    with Pool(max(cpu_count() // 2, 1)) as mp:
        for users in tqdm(mp.imap_unordered(_enumerate_users, input_filenames), total=len(input_filenames), desc="[Identifying Users]", file=sys.stdout):
            usernames.update(users)
    ## Sort
    usernames = [u[0] for u in usernames.most_common()[::-1]]
    ## Filter Irrelevant Users
    usernames = list(filter(lambda i: i is not None and i not in IGNORE_SET, usernames))
    return usernames

def query_user_data(users,
                    output_dir,
                    ignore_existing=False,
                    use_praw=False,
                    start_date=None,
                    end_date=None,
                    max_comments=None,
                    max_submissions=None,
                    get_comments=False,
                    get_submissions=False,
                    users_per_chunk=1):
    """
    
    """
    ## Date Formatting
    if start_date is None:
        start_date = "2015-01-01"
    if end_date is None:
        end_date = datetime.now().date().isoformat()
    ## Format
    users = list(map(lambda user: (user, f"{output_dir}/{user}.submissions.json.gz" if get_submissions else None, f"{output_dir}/{user}.comments.json.gz" if get_comments else None), users))
    ## Filter Existing
    if ignore_existing:
        users = list(map(lambda user: (user[0], None if (user[1] is None or os.path.exists(user[1])) else user[1], None if (user[2] is None or os.path.exists(user[2])) else user[2]), users))
        users = list(filter(lambda i: i[1] is not None or i[2] is not None, users))
    ## Group by Query Type
    users = [[u for u in users if u[1] is not None and u[2] is not None],
             [u for u in users if u[1] is not None and u[2] is None],
             [u for u in users if u[1] is None and u[2] is not None]]
    ## Chunk Users
    users_chunks = flatten([list(chunks(u, users_per_chunk)) for u in users])
    ## Initialize Wrapper
    reddit = Reddit(init_praw=use_praw)
    ## Iterate Through Chunks
    for n, n_users in tqdm(enumerate(users_chunks), total=len(users_chunks), desc="[Query Chunk]", file=sys.stdout):
        ## Parse Chunk
        n_users_sub = [u[0] for u in n_users if u[1] is not None]
        n_users_com = [u[0] for u in n_users if u[2] is not None]
        ## Query Submissions and Comments
        n_users_sub_df, n_users_com_df = None, None
        if len(n_users_sub) != 0:
            n_users_sub_df = reddit.retrieve_author_submissions(author=n_users_sub,
                                                                start_date=start_date,
                                                                end_date=end_date,
                                                                limit=1e6 if max_submissions is None else len(n_users_sub) * max_submissions)
        if len(n_users_com) != 0:
            n_users_com_df = reddit.retrieve_author_comments(author=n_users_com,
                                                             start_date=start_date,
                                                             end_date=end_date,
                                                             limit=1e6 if max_comments is None else len(n_users_com) * max_comments)
        ## Most Recent K Filtering (if Necessary)
        if max_submissions is not None and n_users_sub_df is not None:
            n_users_sub_df = n_users_sub_df.sort_values(["author","created_utc"], ascending=[False,False]).groupby(["author"]).head(max_submissions).reset_index(drop=True)
        if max_comments is not None and n_users_com_df is not None:
            n_users_com_df = n_users_com_df.sort_values(["author","created_utc"], ascending=[False,False]).groupby(["author"]).head(max_comments).reset_index(drop=True)
        ## Cache Data to Disk
        if n_users_sub_df is not None and n_users_sub_df.shape[0] > 0:
            for n_user, n_user_ind in n_users_sub_df.groupby(["author"]).groups.items():
                n_user_sub_df = [json.loads(row.to_json()) for _, row in n_users_sub_df.loc[n_user_ind].iterrows()]
                with gzip.open(f"{output_dir}/{n_user}.submissions.json.gz","wt") as the_file:
                    json.dump(n_user_sub_df, the_file)
        if n_users_com_df is not None and n_users_com_df.shape[0] > 0:
            for n_user, n_user_ind in n_users_com_df.groupby(["author"]).groups.items():
                n_user_com_df = [json.loads(row.to_json()) for _, row in n_users_com_df.loc[n_user_ind].iterrows()]
                with gzip.open(f"{output_dir}/{n_user}.comments.json.gz","wt") as the_file:
                    json.dump(n_user_com_df, the_file)            

def main():
    """
    
    """
    ## Parse Command Line
    LOGGER.warning("[Parsing Command Line]")
    args = parse_command_line()
    ## Get Usernames
    users = enumerate_users(args.inputs)
    ## Run Query
    LOGGER.warning("[Beginning Query Procedure]")
    _ = query_user_data(users=users,
                        output_dir=args.output_dir,
                        ignore_existing=args.ignore_existing,
                        use_praw=args.use_praw,
                        start_date=args.start_date,
                        end_date=args.end_date,
                        max_comments=args.max_comments,
                        max_submissions=args.max_submissions,
                        get_comments=args.get_comments,
                        get_submissions=args.get_submissions,
                        users_per_chunk=args.users_per_chunk)
    LOGGER.warning("[Script Complete]")

######################
### Execution
######################

if __name__ == "__main__":
    _ = main()
