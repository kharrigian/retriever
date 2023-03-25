

#####################
### Imports
#####################

## Standard Libary
import sys
import json
import pytz
import pkgutil
import datetime
import requests
import warnings
from time import sleep
from collections import Counter

## Warning Handling (Unnecessary UserWarnings in PMAW)
warnings.simplefilter("ignore", UserWarning)

## External Libaries
import pandas as pd
from tqdm import tqdm
from praw import Reddit as praw_api
from prawcore import ResponseException
from pmaw import PushshiftAPI as pmaw_api

## Local
from ..util.logging import get_logger
from ..util.helpers import chunks

#####################
### Globals
#####################

## Maximum Number of Results Returnable by Pushshift
MAX_PER_REQUEST = 1000

## Default Maximum Number of Results
DEFAULT_REQUEST_LIMIT = 100

## Logging
LOGGER = get_logger()

## Config File
try:
    CONFIG = json.loads(pkgutil.get_data(__name__, "/../config.json"))
    CONFIG = CONFIG.get("reddit", None)
except FileNotFoundError:
    CONFIG = None

#####################
### Wrapper
#####################

class Reddit(object):

    """
    Reddit Data Retrieval via PMAW and PRAW (optionally)
    """

    def __init__(self,
                 init_praw=False,
                 max_retries=3,
                 backoff=2,
                 allow_praw=True,
                 warn_on_limit=True):
        """
        Initialize a class to retrieve Reddit data based on
        use case and format into friendly dataframes.

        Args:
            init_praw (bool): If True, retrieves data objects 
                    from Reddit API. Requires existence of 
                    config.json with adequate API credentials
                    in home directory
            max_retries (int): Maximum number of query attempts before
                               returning null result
            backoff (int): Baseline number of seconds between failed 
                           query attempts. Increases exponentially with
                           each failed query attempt
            allow_praw (bool): If True (default) and PRAW available,
                               will fallback to using PRAW if not data 
                               detected using PMAW.
            warn_on_limit (bool): If True (default), will warn user whenever
                                  Pushshift's data limit is reached. Can turn 
                                  off if you expect this based on your queries.
        
        Returns:
            None
        """
        ## Class Attributes
        self._init_praw = init_praw
        self._allow_praw = allow_praw
        self._max_retries = max_retries
        self._backoff = backoff
        self._warn_on_limit = warn_on_limit
        ## Class Working Variables
        self._last_req = None
        self._endpoint = "https://api.pushshift.io/reddit"
        ## Initialize APIs
        self._initialize_api_wrappers()
    
    def __repr__(self):
        """
        Print a description of the class state.

        Args:
            None
        
        Returns:
            desc (str): Class parameters
        """
        desc = "Reddit(init_praw={})".format(self._init_praw)
        return desc

    def _initialize_api_wrappers(self):
        """
        Initialize API Wrappers (PRAW and/or PMAW)

        Args:
            None
        
        Returns:
            None. Sets class api attribute.
        
        Raises:
            prawcore.exceptions.OAuthException: invalid_grant error processing request: This will occur either because
                                                credentials are incorrect, or you have enabled 2-factor authentication.
        """
        if hasattr(self, "_init_praw") and self._init_praw and CONFIG is not None:
            ## Initialize PRAW API
            self._praw = praw_api(**CONFIG)
            ## Authenticate Credentials
            authenticated = self._authenticated(self._praw)
            ## Initialize Pushshift API around PRAW API
            if authenticated:
                self.api = pmaw_api(praw=self._praw)
            else:
                LOGGER.warning("WARNING: Reddit API credentials invalid. Defaulting to Pushshift.io API")
                self._init_praw = False
                self.api = pmaw_api()
        else:
            ## Initialize API Objects
            if self._init_praw:
                self._init_praw = False
                LOGGER.warning("WARNING: Reddit API credentials not detected. Defaulting to Pushshift.io API")
            ## Initialize for Fall-Back Queries
            if CONFIG is not None and self._allow_praw:
                self._praw = praw_api(**CONFIG)
                authenticated = self._authenticated(self._praw)
            else:
                self._praw = None
            ## Initialize PMAW
            self.api = pmaw_api()

    def _authenticated(self,
                       reddit):
        """
        Determine whether the given Reddit instance has valid credentials.
        
        Args:
            reddit (PRAW instance): Initialize instance
        """
        ## Default
        auth = True
        ## Make Authentication Attempt
        try:
            reddit.user.me()
        except ResponseException:
            auth = False
        ## Return Status
        return auth
                    
    def _get_start_date(self,
                        start_date_iso=None):
        """
        Get start date epoch

        Args:
            start_date_iso (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to start of Reddit
        
        Returns:
            start_epoch (int): Start date in form of epoch
        """
        ## If Necessary, Use Default Start Date
        if start_date_iso is None:
            start_date_iso = "2005-08-01"
        ## Parse and Localize
        start_date_dt = pd.to_datetime(start_date_iso)
        start_date_dt = pytz.utc.localize(start_date_dt)
        ## Translate to Epoch
        start_epoch = int(start_date_dt.timestamp())
        return start_epoch
    
    def _get_end_date(self,
                      end_date_iso=None):
        """
        Get end date epoch

        Args:
            end_date_iso (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to current date
        
        Returns:
            end_epoch (int): End date in form of epoch
        """
        ## If Necessary, Use Tomorrow as Default End Date
        if end_date_iso is None:
            end_date_iso = (datetime.datetime.now().date() + datetime.timedelta(1)).isoformat()
        ## Parse and Localize
        end_date_dt = pd.to_datetime(end_date_iso)
        end_date_dt = pytz.utc.localize(end_date_dt)
        ## Translate to Epoch
        end_epoch = int(end_date_dt.timestamp())
        return end_epoch
    
    def _parse_date_frequency(self,
                              freq):
        """
        Convert str-formatted frequency into seconds. Base frequencies
        include seconds (s), minutes (m), hours (h), days (d), weeks (w),
        months (mo), and years (y).

        Args:
            freq (str): "{int}{base_frequency}"
        
        Returns:
            period (int): Time in seconds associated with frequency
        """
        ## Frequencies in terms of seconds
        base_freqs = {
            "m":60,
            "h":60 * 60,
            "d":60 * 60 * 24,
            "w":60 * 60 * 24 * 7,
            "mo":60 * 60 * 24 * 31,
            "y":60 * 60 * 24 * 365
        }
        ## Parse String
        freq = freq.lower()
        freq_ind = 0
        while freq_ind < len(freq) - 1 and freq[freq_ind].isdigit():
            freq_ind += 1
        mult = 1
        if freq_ind > 0:
            mult = int(freq[:freq_ind])
        base_freq = freq[freq_ind:]
        if base_freq not in base_freqs:
            raise ValueError("Could not parse frequency.")
        period = mult * base_freqs.get(base_freq)
        return period

    def _chunk_timestamps(self,
                          start_epoch,
                          end_epoch,
                          chunksize):
        """

        """
        if chunksize is None:
            time_chunks = [start_epoch, end_epoch]
        else:
            time_chunksize = self._parse_date_frequency(chunksize)
            time_chunks = [start_epoch]
            while time_chunks[-1] < end_epoch:
                time_chunks.append(min(time_chunks[-1] + time_chunksize, end_epoch))
        return time_chunks
    
    def _parse_pmaw_submission_request(self,
                                       request):
        """
        Retrieve submission search data and format into 
        a standard pandas dataframe format

        Args:
            request (generator): self.api.search_submissions response
        
        Returns:
            df (pandas DataFrame): Submission search data
        """
        ## Define Variables of Interest
        data_vars = ["archived",
                     "author",
                     "author_flair_text",
                     "author_flair_type",
                     "author_fullname",
                     "category",
                     "comment_limit",
                     "content_categories",
                     "created_utc",
                     "crosspost_parent",
                     "domain",
                     "discussion_type",
                     "distinguished",
                     "downs",
                     "full_link",
                     "gilded",
                     "id",
                     "is_meta",
                     "is_original_content",
                     "is_reddit_media_domain",
                     "is_self",
                     "is_video",
                     "link_flair_text",
                     "link_flair_type",
                     "locked",
                     "media",
                     "num_comments",
                     "num_crossposts",
                     "num_duplicates",
                     "num_reports",
                     "over_18",
                     "permalink",
                     "score",
                     "selftext",
                     "subreddit",
                     "subreddit_id",
                     "thumbnail",
                     "title",
                     "url",
                     "ups",
                     "upvote_ratio"]
        ## Parse Data
        response_formatted = []
        for r in request:
            r_data = {}
            for d in data_vars:
                r_data[d] = r.get(d, None)
            response_formatted.append(r_data)
        ## Format into DataFrame
        df = pd.DataFrame(response_formatted)
        if len(df) > 0:
            df = df.sort_values("created_utc", ascending=True)
            df = df.reset_index(drop=True)
        return df
    
    def _parse_pmaw_comment_request(self,
                                    request):
        """
        Retrieve comment search data and format into 
        a standard pandas dataframe format

        Args:
            request (generator): self.api.search_comments response
        
        Returns:
            df (pandas DataFrame): Comment search data
        """
        ## Define Variables of Interest
        data_vars = [
                    "author",
                    "author_flair_text",
                    "author_flair_type",
                    "author_fullname",
                    "body",
                    "collapsed",
                    "collapsed_reason",
                    "controversiality",
                    "created_utc",
                    "downs",
                    "edited",
                    "gildings",
                    "id",
                    "is_submitter",
                    "link_id",
                    "locked",
                    "parent_id",
                    "permalink",
                    "stickied",
                    "subreddit",
                    "subreddit_id",
                    "score",
                    "score_hidden",
                    "total_awards_received",
                    "ups"
        ]
        ## Parse Data
        response_formatted = []
        for r in request:
            r_data = {}
            for d in data_vars:
                r_data[d] = None
                if isinstance(r, dict):
                    d_obj = r.get(d)
                else:
                    d_obj = getattr(r, d)
                if d_obj is None:
                    continue
                if d == "author" and not isinstance(d_obj, str):
                    d_obj = d_obj.name
                if d == "created_utc":
                    d_obj = int(d_obj)
                if d == "subreddit" and not isinstance(d_obj,str):
                    d_obj = d_obj.display_name
                if d == "link_id" and not isinstance(d_obj, str) and hasattr(r, "permalink"):
                    d_obj = getattr(r, "permalink")
                    if d_obj is not None:
                        d_obj = d_obj.split("/comments/")[1].split("/")[0]
                ## NOTE: As of July 12, 2022 - link_id, author_fullname, and parent_id are not returned in appropriate format  for some data
                r_data[d] = d_obj
            response_formatted.append(r_data)
        ## Format into DataFrame
        df = pd.DataFrame(response_formatted)
        if len(df) > 0:
            df = df.sort_values("created_utc", ascending=True)
            df = df.reset_index(drop=True)
        return df

    def _getSubComments(self,
                        comment,
                        allComments):
        """
        Helper to recursively expand comment trees from PRAW

        Args:
            comment (comment Object): A given PRAW comment
            allComments (list): Stash of expanded comment objects
        
        Returns:
            None, appends allComments inplace recursively
        """
        ## Append Comment
        allComments.append(comment)
        ## Get Replies
        if not hasattr(comment, "replies"):
            replies = comment.comments()
        else:
            replies = comment.replies
        ## Recurse
        for child in replies:
            self._getSubComments(child, allComments)

    def _retrieve_submission_comments_praw(self,
                                           submission_id):
        """
        Retrieve comments recursively from a submission using PRAW

        Args:
            submission_id (str): ID for a reddit submission
        
        Returns:
            comment_df (pandas DataFrame): All comments and metadata from the submission.
        """
        ## Retrieve Submission
        sub = self._praw.submission(submission_id)
        ## Initialize Comment List
        comments = sub.comments
        ## Recursively Expand Comment Forest
        commentsList = []
        for comment in comments:
            self._getSubComments(comment, commentsList)
        ## Ignore Comment Forest Artifacts
        commentsList = [c for c in commentsList if "MoreComments" not in str(type(c))]
        ## Parse
        if len(commentsList) > 0:
            comment_df = self._parse_pmaw_comment_request(commentsList)
            return comment_df
        ## Return Null
        return None
    
    def _parse_metadata(self,
                        metadata):
        """
        Parse the subreddit metadata variables, only extracting
        fields we care about.

        Args:
            metadata (dict): Dictionary of metadata variables
        
        Returns:
            metadata (dict): Subset of metadata fields and values we care about.
        """
        metadata_columns = ["display_name",
                            "restrict_posting",
                            "wiki_enabled",
                            "title",
                            "primary_color",
                            "active_user_count",
                            "display_name_prefixed",
                            "accounts_active",
                            "public_traffic",
                            "subscribers",
                            "name",
                            "quarantine",
                            "hide_ads",
                            "emojis_enabled",
                            "advertiser_category",
                            "public_description",
                            "spoilers_enabled",
                            "all_original_content",
                            "key_color",
                            "created",
                            "submission_type",
                            "allow_videogifs",
                            "allow_polls",
                            "collapse_deleted_comments",
                            "allow_discovery",
                            "link_flair_enabled",
                            "subreddit_type",
                            "suggested_comment_sort",
                            "id",
                            "over18",
                            "description",
                            "restrict_commenting",
                            "allow_images",
                            "lang",
                            "whitelist_status",
                            "url",
                            "created_utc"]
        metadata = {c:metadata.get(c,None) for c in metadata_columns}
        return metadata
                          
    def retrieve_subreddit_metadata(self,
                                    subreddit):
        """
        Retrieve metadata for a given subreddit (e.g. subscribers, description)

        Args:
            subreddit (str): Name of the subreddit
        
        Returns:
            metadata_clean (dict): Dictionary of metadata for the subreddit
        """
        ## Validate Configuration
        if not self._init_praw:
            raise ValueError("Must have initialized class with PRAW to access subreddit metadata")
        ## Reset Backoff
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        ## Default Output
        metadata_clean = None
        ## Load Object and Fetch Metadata
        for _ in range(self._max_retries):
            try:
                sub = self._praw.subreddit(subreddit)
                sub._fetch()
                ## Parse
                metadata = vars(sub)
                metadata_clean = self._parse_metadata(metadata)
                ## Success: Exit
                break
            except Exception as e:
                LOGGER.warning(e)
                sleep(backoff)
                backoff = 2 ** backoff
        ## Return
        return metadata_clean
    
    def retrieve_subreddit_submissions(self,
                                       subreddit,
                                       start_date=None,
                                       end_date=None,
                                       limit=DEFAULT_REQUEST_LIMIT,
                                       cols=None,
                                       chunksize=None):
        """
        Retrieve submissions for a particular subreddit

        Args:
            subreddit (str): Canonical name of the subreddit
            start_date (str or None): If str, expected
                    to be parsed by pandas.to_datetime. None
                    defaults to beginning of Reddit.
            end_date (str or None):  If str, expected
                    to be parse by pandas.to_datetime. None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
            cols (list or None): Optional Filters
            chunksize (str): Date frequency
        
        Returns:
            df (pandas dataframe): Submission search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Chunk Queries into Time Periods
        time_chunks = self._chunk_timestamps(start_epoch,
                                             end_epoch,
                                             chunksize)
        ## Rest Backoff and Retries
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        ## Make Query Attempt
        total = 0
        df_all = []
        for tcstart, tcstop in zip(time_chunks[:-1], time_chunks[1:]):
            ## Case 0: Limit Reached
            if limit is not None and total >= limit:
                break
            ## Case 1: Attempt Until Retry Limit Reached
            for _ in range(retries):
                try:
                    ## Construct Call
                    query_params = {
                        "since":tcstart,
                        "until":tcstop+1,
                        "subreddit":subreddit,
                        "limit":min(limit, MAX_PER_REQUEST) if limit is not None else MAX_PER_REQUEST,
                    }
                    if cols is not None:
                        query_params["filter"] = cols
                    ## Make Request
                    req = self.api.search_submissions(**query_params)
                    ## Retrieve
                    df = self._parse_pmaw_submission_request(req)
                    ## Format 
                    if len(df) > 0:
                        ## Format
                        df = df.sort_values("created_utc", ascending=True)
                        df = df.reset_index(drop=True)
                        ## Cache
                        df_all.append(df)
                        ## Update Count
                        total += len(df)
                        ## Length Limit Warning
                        if df.shape[0] == MAX_PER_REQUEST:
                            if self._warn_on_limit:
                                LOGGER.warning("WARNING: Maximum result limit reached for time range: {} to {}. Consider reducing the 'chunksize' to query smaller time windows.".format(tcstart, tcstop))
                    ## Success: Break
                    break
                except Exception as e:
                    LOGGER.warning(e)
                    sleep(backoff)
                    backoff = 2 ** backoff
        ## Length Check
        if len(df_all) == 0:
            return None
        ## Concatenate
        df_all = pd.concat(df_all).reset_index(drop=True)
        ## Reduce
        if limit is not None and len(df_all) > limit:
            df_all = df_all.iloc[:limit].copy()
        ## Limit to specified columns
        if cols:
            df_all = df_all.loc[:, [c for c in cols if c in df_all.columns]] 
        return df_all
    
    def _retrieve_submission_comments(self,
                                      submission,
                                      comment_ids=[],
                                      start_date=None,
                                      end_date=None,
                                      last_req=None,
                                      wait_time=2,
                                      max_attempts=3,
                                      backoff=2,
                                      convert_id_to_int=True):
        """
        Recursive identification of comment IDs for a submission
        """
        ## Start/End Date
        if start_date is None:
            start_date = self._get_start_date(None)
        if end_date is None:
            end_date = self._get_end_date(None)
        ## Submission Formatting
        if isinstance(submission, str):
            submission = [submission]
        submission = list(map(lambda i: i if not i.startswith("t3_") else i[3:], submission))
        ## Temporary -- Need to convert to base 10 due to Pushshift Conversion Issues
        if convert_id_to_int:
            submission = list(map(lambda i: str(int(i, 36)), submission))
        ## Merge Submissions
        submission = ",".join(submission)
        ## Format Query
        search_req = f"{self._endpoint}/comment/search/?size=100&fields=id&q=*&link_id={submission}&until={end_date}&since={start_date}"
        ## Waiting (For Rate Limiting)
        if last_req is None and self._last_req is not None:
            last_req = self._last_req
        if last_req is not None:
            since_last = (datetime.datetime.now()-last_req).total_seconds()
            _ = sleep(max(0, wait_time - since_last))
        ## Track Last Request Time (Class Wide)
        last_req = datetime.datetime.now()
        self._last_req = last_req
        ## Execute Query
        attempted = 0
        attempt_wait = wait_time
        while True:
            ## Check Exit Criteria
            if attempted == max_attempts:
                LOGGER.warning("WARNING: Comment ID warning: Collection stopped after {} attempts.".format(max_attempts))
                return list(set(comment_ids))
            ## Make Request
            resp = requests.get(search_req)
            ## Parse Request
            if resp.status_code != 200:
                ## Too many requests (Backoff Silently)
                if resp.status_code == 429:
                    attempted += 1
                    attempt_wait = attempt_wait * backoff
                    _ = sleep(attempt_wait)
                ## Something Else (Exit)
                else:
                    LOGGER.warning("WARNING: Comment ID warning: Got Non 200 Request Code {}: {}".format(resp.status_code, resp.reason))
                    return list(set(comment_ids))
            else:
                ## Success
                break
        ## Get Data
        resp_ids = [i.get("id") for i in resp.json()["data"]]
        ## Case 1: Fewer than Limit Returned
        if len(resp_ids) < 100:
            comment_ids.extend(resp_ids)
        ## Case 2: More Than Limit Returned, Break Up (Binary Search)
        else:
            ## Split Date Range in Half
            date_bounds = [start_date, int((start_date+end_date)/2), end_date]
            ## Query Each Range Separately and Recursively
            for dstart, dend in zip(date_bounds[:-1], date_bounds[1:]):
                ## Run Search
                _ = self._retrieve_submission_comments(submission=submission,
                                                       comment_ids=comment_ids,
                                                       start_date=dstart,
                                                       end_date=dend,
                                                       wait_time=wait_time,
                                                       last_req=last_req,
                                                       convert_id_to_int=False)
        ## Return
        return list(set(comment_ids))
    
    def retrieve_submission_comments(self,
                                     submission,
                                     start_date=None,
                                     end_date=None):
        """
        Retrieve comments for a particular submission

        Args:
            submission (str): Canonical name of the submission
            start_date (str): Lower date boundary (helpful for large submissions)
            end_date (str): Upper date boundary

        Returns:
            df (pandas dataframe): Comment search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## ID Extraction
        if not isinstance(submission, list):
            submission = [submission]
        submissions_clean = []
        for s in submission:
            if "https" in s or "reddit" in s:
                s = s.split("comments/")[1].split("/")[0]
            if s.startswith("t3_"):
                s = s.replace("t3_","")
            submissions_clean.append(s)
        ## Init Cache
        comment_data = []
        missing_submissions = submissions_clean
        ## PMAW Search
        if not self._init_praw or self._init_praw and (hasattr(self, "_praw") and self._praw is None):
            ## Retrieve Comment IDs
            comment_ids = self._retrieve_submission_comments(submissions_clean,
                                                             start_date=start_epoch,
                                                             end_date=end_epoch,
                                                             wait_time=2,
                                                             backoff=4,
                                                             convert_id_to_int=True)
            ## Retrieve Comments
            comment_data = []
            for ids_chunk in chunks(comment_ids, 100): ## Note this is a limit set by Pushshift
                ## Init Request
                dreq = self.api.search_comments(ids=ids_chunk, metadata=True, limit=100)
                ## Parse Request
                dreq_df = self._parse_pmaw_comment_request(dreq)
                ## Check Parse and Cache
                if dreq_df is not None and len(dreq_df) > 0:
                    comment_data.append(dreq_df)
            ## Merge and Format
            if len(comment_data) > 0:
                comment_data = pd.concat(comment_data, axis=0, sort=False)
                comment_data = comment_data.sort_values("created_utc", ascending=True)
                comment_data = comment_data.reset_index(drop=True)
            ## Determine Which Submissions Don't Have any Comments
            if isinstance(comment_data, list):
                missing_submissions = submissions_clean
            else:
                missing_submissions = list(set(submissions_clean) - set(comment_data["link_id"]))
        ## Fall Back to PRAW
        if len(missing_submissions) > 0 and hasattr(self, "_praw") and self._praw is not None and self._allow_praw:
            ## Iterate through missing
            comment_data_praw = []
            for s in missing_submissions:
                comment_data_praw.append(self._retrieve_submission_comments_praw(submission_id=s))
            ## Filter
            comment_data_praw = list(filter(lambda d: d is not None, comment_data_praw))
            ## Format
            if len(comment_data_praw) > 0:
                comment_data_praw = pd.concat(comment_data_praw).reset_index(drop=True)
            ## Merge
            if len(comment_data) > 0 and len(comment_data_praw) > 0:
                comment_data = pd.concat([comment_data, comment_data_praw], axis=0)
            elif len(comment_data) > 0 and len(comment_data_praw) == 0:
                pass
            elif len(comment_data) == 0 and len(comment_data_praw) > 0:
                comment_data = comment_data_praw
            elif len(comment_data) == 0 and len(comment_data_praw) == 0:
                pass
            ## Sort
            comment_data = comment_data.sort_values("created_utc",ascending=True)
            comment_data = comment_data.reset_index(drop=True)
        ## Deduplicate
        if len(comment_data) > 0:
            comment_data = comment_data.drop_duplicates(subset=["id"],keep="last").reset_index(drop=True)
        ## Return
        return comment_data
    
    def retrieve_author_comments(self,
                                 author,
                                 start_date=None,
                                 end_date=None,
                                 limit=DEFAULT_REQUEST_LIMIT,
                                 chunksize=None):
        """
        Retrieve comments for a particular Reddit user. Does not
        return user-authored submissions (e.g. self-text)

        Args:
            author (str): Username of the redditor
            start_date (str or None): If str, expected
                    to be parsed by pandas.to_datetime. None
                    defaults to beginning of Reddit.
            end_date (str or None):  If str, expected
                    to be parse by pandas.to_datetime. None, 
                    defaults to current date
            limit (int): Maximum number of comments to 
                    retrieve
            chunksize (str or None): Date frequency for breaking up queries
        
        Returns:
            df (pandas dataframe): Comment search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Chunk Queries into Time Periods
        time_chunks = self._chunk_timestamps(start_epoch,
                                             end_epoch,
                                             chunksize)
        ## Make Query Attempt
        df_all = []
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        total = 0
        for tcstart, tcstop in zip(time_chunks[:-1], time_chunks[1:]):
            ## Check Limit
            if limit is not None and total >= limit:
                break
            ## Run Multiple Attempts
            for _ in range(retries):
                try:
                    ## Construct Call
                    query_params = {"since":tcstart,
                                    "until":tcstop+1,
                                    "limit":min(limit, MAX_PER_REQUEST) if limit is not None else MAX_PER_REQUEST,
                                    "author":author}
                    ## Construct Call
                    req = self.api.search_comments(**query_params)
                    ## Retrieve and Parse Data
                    df = self._parse_pmaw_comment_request(req)
                    if len(df) > 0:
                        ## Format
                        df = df.sort_values("created_utc", ascending=True)
                        df = df.reset_index(drop=True)
                        ## Cache
                        df_all.append(df)
                        ## Update Count
                        total += len(df)
                        ## Length Limit Warning
                        if df.shape[0] == MAX_PER_REQUEST:
                            if self._warn_on_limit:
                                LOGGER.warning("WARNING: Maximum result limit reached for time range: {} to {}. Consider reducing the 'chunksize' to query smaller time windows.".format(tcstart, tcstop))
                    ## Sucess: Break
                    break
                except Exception as e:
                    LOGGER.warning(e)
                    sleep(backoff)
                    backoff = 2 ** backoff
        ## Length Check
        if len(df_all) == 0:
            return None
        ## Merge
        df_all = pd.concat(df_all).reset_index(drop=True)
        ## Reduce
        if limit is not None and len(df_all) > limit:
            df_all = df_all.iloc[:limit].copy()
        return df_all

    def retrieve_author_submissions(self,
                                    author,
                                    start_date=None,
                                    end_date=None,
                                    limit=DEFAULT_REQUEST_LIMIT,
                                    chunksize=None):
        """
        Retrieve submissions for a particular Reddit user. Does not
        return user-authored comments

        Args:
            author (str): Username of the redditor
            start_date (str or None): If str, expected
                    to be parsed by pandas.to_datetime. None
                    defaults to beginning of Reddit.
            end_date (str or None):  If str, expected
                    to be parse by pandas.to_datetime. None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
            chunksize (str or None): Date frequency for breaking up queries

        Returns:
            df (pandas dataframe): Comment search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Chunk Queries into Time Periods
        time_chunks = self._chunk_timestamps(start_epoch,
                                             end_epoch,
                                             chunksize)
        ## Make Queries
        df_all = []
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        total = 0
        for tcstart, tcstop in zip(time_chunks[:-1], time_chunks[1:]):
            ## Check Limit
            if limit is not None and total >= limit:
                break
            for _ in range(retries):
                try:
                    ## Construct Call
                    query_params = {"until":tcstop+1,
                                    "since":tcstart,
                                    "limit":min(limit, MAX_PER_REQUEST) if limit is not None else MAX_PER_REQUEST,
                                    "author":author}
                    ## Construct Call
                    req = self.api.search_submissions(**query_params)
                    ## Retrieve and Parse Data
                    df = self._parse_pmaw_submission_request(req)
                    if len(df) > 0:
                        ## Sort and Format
                        df = df.sort_values("created_utc", ascending=True)
                        df = df.reset_index(drop=True)
                        ## Cache
                        df_all.append(df)
                        ## Length Update
                        total += len(df)
                        ## Length Limit Warning
                        if df.shape[0] == MAX_PER_REQUEST:
                            if self._warn_on_limit:
                                LOGGER.warning("WARNING: Maximum result limit reached for time range: {} to {}. Consider reducing the 'chunksize' to query smaller time windows.".format(tcstart, tcstop))
                    ## Success: Break
                    break
                except Exception as e:
                    LOGGER.warning(e)
                    sleep(backoff)
                    backoff = 2 ** backoff
        ## Length Check
        if len(df_all) == 0:
            return None
        ## Concatenate
        df_all = pd.concat(df_all).reset_index(drop=True)
        ## Reduce
        if limit is not None and len(df_all) > limit:
            df_all = df_all.iloc[:limit].copy()
        return df_all

    def search_for_submissions(self,
                               query=None,
                               subreddit=None,
                               start_date=None,
                               end_date=None,
                               limit=DEFAULT_REQUEST_LIMIT):
        """
        Search for submissions based on title

        Args:
            query (str): Title query
            subreddit (str or None): Additional filtering by subreddit.
            start_date (str or None): If str, expected
                    to be parsed by pandas.to_datetime. None
                    defaults to beginning of Reddit.
            end_date (str or None):  If str, expected
                    to be parse by pandas.to_datetime. None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
        
        Returns:
            df (pandas dataframe): Submission search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Construct Query
        query_params = {
            "until":end_epoch,
            "since":start_epoch,
            "limit":min(limit, MAX_PER_REQUEST) if limit is not None else MAX_PER_REQUEST
        }
        if query is not None:
            query_params["title"] = '"{}"'.format(query)
        if subreddit is not None:
            query_params["subreddit"] = subreddit
        ## Make Query Attempt
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_submissions(**query_params)
                ## Retrieve and Parse Data
                df = self._parse_pmaw_submission_request(req)
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
                return df
            except Exception as e:
                LOGGER.warning(e)
                sleep(backoff)
                backoff = 2 ** backoff
    
    def search_for_comments(self,
                            query=None,
                            subreddit=None,
                            start_date=None,
                            end_date=None,
                            limit=DEFAULT_REQUEST_LIMIT):
        """
        Search for comments based on text in body

        Args:
            query (str): Comment query
            subreddit (str or None): Additional filtering by subreddit.
            start_date (str or None): If str, expected
                    to be parsed by pandas.to_datetime. None
                    defaults to beginning of Reddit.
            end_date (str or None):  If str, expected
                    to be parse by pandas.to_datetime. None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
        
        Returns:
            df (pandas dataframe): Comment search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Construct Query
        query_params = {
            "until":end_epoch,
            "since":start_epoch,
            "limit":min(limit, MAX_PER_REQUEST) if limit is not None else MAX_PER_REQUEST
        }
        if subreddit is not None:
            query_params["subreddit"] = subreddit
        if query is not None:
            query_params["q"] = query
        ## Reset Backoff + Retry Counter
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        ## Make Query Attempt
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_comments(**query_params)
                ## Retrieve and Parse Data
                df = self._parse_pmaw_comment_request(req)
                ## Format
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
                    ## Length Check
                    if df.shape[0] == MAX_PER_REQUEST:
                        if self._warn_on_limit:
                            LOGGER.warning("WARNING: Maximum result limit reached for time range: {} to {}. Consider reducing the 'chunksize' to query smaller time windows.".format(start_epoch, end_epoch))
                ## Return
                return df
            except Exception as e:
                LOGGER.warning(e)
                sleep(backoff)
                backoff = 2 ** backoff
    
    def identify_active_subreddits(self,
                                   start_date=None,
                                   end_date=None,
                                   chunksize="5m"):
        """
        Identify active subreddits based on submission histories

        Args:
            start_date (str or None): If str, expected
                    to be parsed by pandas.to_datetime. None
                    defaults to beginning of Reddit.
            end_date (str or None):  If str, expected
                    to be parse by pandas.to_datetime. None, 
                    defaults to current date
            search_freq (int): Minutes to consider per request. Lower frequency 
                               means better coverage but longer query time.
        
        Returns:
            subreddit_count (pandas Series): Subreddit, Submission Count in Time Period
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Create Search Range
        time_chunks = self._chunk_timestamps(start_epoch,
                                             end_epoch,
                                             chunksize)
        ## Query Subreddits
        subreddit_count = Counter()
        for start, stop in tqdm(zip(time_chunks[:-1], time_chunks[1:]), total = len(time_chunks)-1, file=sys.stdout):
            ## Make Get Request
            req = f"{self._endpoint}/search/submission/?since={start}&until={stop}&filter=subreddit&size=1000"
            ## Reset Backoff/Attempt Count
            backoff = self._backoff if hasattr(self, "_backoff") else 2
            retries = self._max_retries if hasattr(self, "_max_retries") else 3
            ## Cycle Through Attempts
            for _ in range(retries):
                try:
                    resp = requests.get(req)
                    ## Parse Request
                    if resp.status_code == 200:
                        ## Get Data
                        data = resp.json()["data"]
                        ## Length Check
                        if len(data) == MAX_PER_REQUEST:
                            if self._warn_on_limit:
                                LOGGER.warning("WARNING: Maximum result limit reached for time range: {} to {}. Consider reducing the 'chunksize' to query smaller time windows.".format(start, stop))   
                        ## Count Subreddits
                        sub_count = Counter([i["subreddit"] for i in data])
                        ## Update
                        subreddit_count = subreddit_count + sub_count
                        ## Sleep
                        sleep(backoff)
                        ## Success: Move On
                        break
                    else:
                        ## Sleep with exponential backoff
                        sleep(backoff)
                        backoff = 2 ** backoff
                except Exception as e:
                    LOGGER.warning(e)
                    sleep(backoff)
                    backoff = 2 ** backoff
        ## Format
        subreddit_count = pd.Series(subreddit_count).sort_values(ascending=False)
        ## Drop User-Subreddits
        subreddit_count = subreddit_count.loc[subreddit_count.index.map(lambda i: not i.startswith("u_"))]
        return subreddit_count

    def retrieve_subreddit_user_history(self,
                                        subreddit,
                                        start_date=None,
                                        end_date=None,
                                        history_type="comment",
                                        chunksize=None):
        """
        Args:
            subreddit (str): Subreddit of interest
            start_date (str or None): If str, expected
                    to be parsed by pandas.to_datetime. None
                    defaults to beginning of Reddit.
            end_date (str or None):  If str, expected
                    to be parse by pandas.to_datetime. None, 
                    defaults to current date
            history_type (str): "comment" or "submission": Type of post to get author counts for
            docs_per_chunk (int): How many documents to retrieve at a time. Larger chunks means slower queries
                                 and higher potential failure rate.
        
        Returns:
            authors (Series): Author post counts in subreddit. Ignores deleted authors
                              and attempts to filter out bots
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Chunk Queries into Time Periods
        time_chunks = self._chunk_timestamps(start_epoch,
                                             end_epoch,
                                             chunksize)
        ## Endpoint
        if history_type == "comment":
            endpoint = self.api.search_comments
        elif history_type == "submission":
            endpoint = self.api.search_submissions
        else:
            raise ValueError("history_type parameter must be either comment or submission")
        ## Query Authors
        authors = Counter()
        for start, stop in tqdm(zip(time_chunks[:-1], time_chunks[1:]), total=len(time_chunks)-1, file=sys.stdout):
            ## Reset Retries/Backoff
            backoff = self._backoff if hasattr(self, "_backoff") else 2
            retries = self._max_retries if hasattr(self, "_max_retries") else 3
            ## Attempt UJntil Success
            for _ in range(retries):
                try:
                    ## Make Request
                    req = endpoint(subreddit=subreddit,
                                   since=start,
                                   until=stop,
                                   filter="author")
                    ## Isolate Author
                    resp = []
                    for a in req:
                        if isinstance(a, dict):
                            resp.append(a.get("author"))
                        else:
                            resp.append(a.author)
                    ## Length Check
                    if len(resp) == MAX_PER_REQUEST:
                        if self._warn_on_limit:
                            LOGGER.warning("WARNING: Maximum result limit reached for time range: {} to {}. Consider reducing the 'chunksize' to query smaller time windows.".format(start, stop))
                    ## Filtering
                    resp = list(filter(lambda i: i is not None and i != "[deleted]" and i != "[removed]" and not i.lower().endswith("bot"), resp))
                    ## Update Counts
                    ac = Counter(resp)
                    authors += ac
                    ## Success: Move to Next Chunk
                    break
                except Exception as e:
                    LOGGER.warning(e)
                    sleep(backoff)
                    backoff = 2 ** backoff
        ## Format
        authors = pd.Series(authors).sort_values(ascending=False)
        return authors

    def convert_utc_epoch_to_datetime(self,
                                      epoch):
        """
        Convert an integer epoch time to a datetime

        Args:
            epoch (int): UTC epoch time
        
        Returns:
            conversion (datetime): Datetime object
        """
        ## Convert epoch to datetime
        conversion = datetime.datetime.utcfromtimestamp(epoch)
        return conversion
