

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
from time import sleep
from collections import Counter

## External Libaries
import pandas as pd
from tqdm import tqdm
from praw import Reddit as praw_api
from prawcore import ResponseException
from psaw import PushshiftAPI as psaw_api

## Local
from ..util.logging import get_logger

#####################
### Globals
#####################

## Default Maximum Number of Results
REQUEST_LIMIT = 100000

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
    Reddit Data Retrieval via PSAW and PRAW (optionally)
    """

    def __init__(self,
                 init_praw=False,
                 max_retries=3,
                 backoff=2):
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
        
        Returns:
            None
        """
        ## Class Attributes
        self._init_praw = init_praw
        self._max_retries = max_retries
        self._backoff = backoff
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
        Initialize API Wrappers (PRAW and/or PSAW)

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
                self.api = psaw_api(self._praw, max_results_per_request=100)
            else:
                LOGGER.warning("Reddit API credentials invalid. Defaulting to Pushshift.io API")
                self._init_praw = False
                self.api = psaw_api(max_results_per_request=100)
        else:
            ## Initialize API Objects
            if self._init_praw:
                self._init_praw = False
                LOGGER.warning("Reddit API credentials not detected. Defaulting to Pushshift.io API")
            ## Initialize for Fall-Back Queries
            if CONFIG is not None:
                self._praw = praw_api(**CONFIG)
                authenticated = self._authenticated(self._praw)
            else:
                self._praw = None
            ## Initialize PSAW
            self.api = psaw_api(max_results_per_request=100)

    def _authenticated(self,
                       reddit):
        """
        Determine whether the given Reddit instance has valid credentials.
        
        Args:
            reddit (PRAW instance): Initialize instance
        """
        try:
            reddit.user.me()
        except ResponseException:
            return False
        else:
            return True
                    

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
        if start_date_iso is None:
            start_date_iso = "2005-08-01"
        start_date_dt = pd.to_datetime(start_date_iso)
        start_date_dt = pytz.utc.localize(start_date_dt)
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
        if end_date_iso is None:
            end_date_iso = (datetime.datetime.now().date() + \
                            datetime.timedelta(1)).isoformat()
        end_date_dt = pd.to_datetime(end_date_iso)
        end_date_dt = pytz.utc.localize(end_date_dt)
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
    
    def _parse_psaw_submission_request(self,
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
            if not hasattr(self, "_init_praw") or not self._init_praw:
                for d in data_vars:
                    r_data[d] = None
                    if hasattr(r, d):
                        r_data[d] = getattr(r, d)
            else:
                for d in data_vars:
                    r_data[d] = None
                    if hasattr(r, d):
                        d_obj = getattr(r, d)
                        if d_obj is None:
                            continue
                        if d == "author":
                            d_obj = d_obj.name
                        if d == "created_utc":
                            d_obj = int(d_obj)
                        if d == "subreddit":
                            d_obj = d_obj.display_name
                        r_data[d] = d_obj
            response_formatted.append(r_data)
        ## Format into DataFrame
        df = pd.DataFrame(response_formatted)
        if len(df) > 0:
            df = df.sort_values("created_utc", ascending=True)
            df = df.reset_index(drop=True)
        return df
    
    def _parse_psaw_comment_request(self,
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
                if hasattr(r, d):
                    d_obj = getattr(r, d)
                    if d_obj is None:
                        continue
                    if d == "author" and not isinstance(d_obj, str):
                        d_obj = d_obj.name
                    if d == "created_utc":
                        d_obj = int(d_obj)
                    if d == "subreddit" and not isinstance(d_obj,str):
                        d_obj = d_obj.display_name
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
            comment_df = self._parse_psaw_comment_request(commentsList)
            return comment_df
    
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
        metadata = dict((c, metadata[c]) for c in metadata_columns)
        return metadata
                          
    def retrieve_subreddit_metadata(self,
                                    subreddit):
        """
        Retrive metadata for a given subreddit (e.g. subscribers, description)

        Args:
            subreddit (str): Name of the subreddit
        
        Returns:
            metadata_clean (dict): Dictioanry of metadata for the subreddit
        """
        ## Validate Configuration
        if not self._init_praw:
            raise ValueError("Must have initialized class with PRAW to access subreddit metadata")
        ## Load Object and Fetch Metadata
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        for _ in range(self._max_retries):
            try:
                sub = self._praw.subreddit(subreddit)
                sub._fetch()
                ## Parse
                metadata = vars(sub)
                metadata_clean = self._parse_metadata(metadata)
                return metadata_clean
            except Exception as e:
                LOGGER.warning(e)
                sleep(backoff)
                backoff = 2 ** backoff
    
    def retrieve_subreddit_submissions(self,
                                       subreddit,
                                       start_date=None,
                                       end_date=None,
                                       limit=REQUEST_LIMIT,
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
        ## Make Query Attempt
        df_all = []
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        total = 0
        for tcstart, tcstop in zip(time_chunks[:-1], time_chunks[1:]):
            if limit is not None and total >= limit:
                break
            for _ in range(retries):
                try:
                    ## Construct Call
                    query_params = dict(after=tcstart,
                                        before=tcstop+1,
                                        subreddit=subreddit,
                                        limit=limit)
                    if cols is not None:
                        query_params["filter"] = cols
                    req = self.api.search_submissions(**query_params)
                    ## Retrieve and Parse Data
                    df = self._parse_psaw_submission_request(req)
                    if len(df) > 0:
                        df = df.sort_values("created_utc", ascending=True)
                        df = df.reset_index(drop=True)
                        df_all.append(df)
                        total += len(df)
                    break
                except Exception as e:
                    LOGGER.warning(e)
                    sleep(backoff)
                    backoff = 2 ** backoff
        if len(df_all) == 0:
            return
        df_all = pd.concat(df_all).reset_index(drop=True)
        if limit is not None and len(df_all) > limit:
            df_all = df_all.iloc[:limit].copy()
        return df_all
    
    def retrieve_submission_comments(self,
                                     submission):
        """
        Retrieve comments for a particular submission

        Args:
            submission (str): Canonical name of the submission

        Returns:
            df (pandas dataframe): Comment search data
        """
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
        ## Make Query Attempt
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_comments(link_id=[f"t3_{s}" for s in submissions_clean])
                ## Retrieve and Parse data
                df = self._parse_psaw_comment_request(req)
                ## Fall Back to PRAW
                if len(df) == 0 and hasattr(self, "_praw") and self._praw is not None:
                    df = []
                    for s in submissions_clean:
                        df.append(self._retrieve_submission_comments_praw(submission_id=s))
                    df = [d for d in df if d is not None]
                    if len(df) > 0:
                        df = pd.concat(df).reset_index(drop=True)
                ## Sort
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
                return df
            except Exception as e:
                LOGGER.warning(e)
                sleep(backoff)
                backoff = 2 ** backoff
    
    def retrieve_author_comments(self,
                                 author,
                                 start_date=None,
                                 end_date=None,
                                 limit=REQUEST_LIMIT,
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
            for _ in range(retries):
                try:
                    ## Construct Call
                    query_params = {"before":tcstop+1,
                                    "after":tcstart,
                                    "limit":limit,
                                    "author":author}
                    ## Construct Call
                    req = self.api.search_comments(**query_params)
                    ## Retrieve and Parse Data
                    df = self._parse_psaw_comment_request(req)
                    if len(df) > 0:
                        df = df.sort_values("created_utc", ascending=True)
                        df = df.reset_index(drop=True)
                        df_all.append(df)
                        total += len(df)
                    break
                except Exception as e:
                    LOGGER.warning(e)
                    sleep(backoff)
                    backoff = 2 ** backoff
        if len(df_all) == 0:
            return
        df_all = pd.concat(df_all).reset_index(drop=True)
        if limit is not None and len(df_all) > limit:
            df_all = df_all.iloc[:limit].copy()
        return df_all

    def retrieve_author_submissions(self,
                                    author,
                                    start_date=None,
                                    end_date=None,
                                    limit=REQUEST_LIMIT,
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
                    query_params = {"before":tcstop+1,
                                    "after":tcstart,
                                    "limit":limit,
                                    "author":author}
                    ## Construct Call
                    req = self.api.search_submissions(**query_params)
                    ## Retrieve and Parse Data
                    df = self._parse_psaw_submission_request(req)
                    if len(df) > 0:
                        df = df.sort_values("created_utc", ascending=True)
                        df = df.reset_index(drop=True)
                        df_all.append(df)
                        total += len(df)
                    break
                except Exception as e:
                    LOGGER.warning(e)
                    sleep(backoff)
                    backoff = 2 ** backoff
        if len(df_all) == 0:
            return
        df_all = pd.concat(df_all).reset_index(drop=True)
        if limit is not None and len(df_all) > limit:
            df_all = df_all.iloc[:limit].copy()
        return df_all

    def search_for_submissions(self,
                               query=None,
                               subreddit=None,
                               start_date=None,
                               end_date=None,
                               limit=REQUEST_LIMIT):
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
            "before":end_epoch,
            "after":start_epoch,
            "limit":limit
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
                df = self._parse_psaw_submission_request(req)
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
                            limit=REQUEST_LIMIT):
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
            "before":end_epoch,
            "after":start_epoch,
            "limit":limit
        }
        if subreddit is not None:
            query_params["subreddit"] = subreddit
        if query is not None:
            query_params["q"] = query
        ## Make Query Attempt
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_comments(**query_params)
                ## Retrieve and Parse Data
                df = self._parse_psaw_comment_request(req)
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
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
        endpoint = "https://api.pushshift.io/reddit/search/submission/"
        subreddit_count = Counter()
        for start, stop in tqdm(zip(time_chunks[:-1], time_chunks[1:]), total = len(time_chunks)-1, file=sys.stdout):
            ## Make Get Request
            req = f"{endpoint}?after={start}&before={stop}&filter=subreddit"
            ## Cycle Through Attempts
            backoff = self._backoff if hasattr(self, "_backoff") else 2
            retries = self._max_retries if hasattr(self, "_max_retries") else 3
            for _ in range(retries):
                try:
                    resp = requests.get(req)
                    ## Parse Request
                    if resp.status_code == 200:
                        data = resp.json()["data"]
                        sub_count = Counter([i["subreddit"] for i in data])
                        subreddit_count = subreddit_count + sub_count
                        sleep(self.api.backoff)
                        break
                    else: ## Sleep with exponential backoff
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
            backoff = self._backoff if hasattr(self, "_backoff") else 2
            retries = self._max_retries if hasattr(self, "_max_retries") else 3
            for _ in range(retries):
                try:
                    req = endpoint(subreddit=subreddit,
                                   after=start,
                                   before=stop,
                                   filter="author")
                    resp = [a.author for a in req]
                    resp = list(filter(lambda i: i != "[deleted]" and i != "[removed]" and not i.lower().endswith("bot"), resp))
                    ac = Counter(resp)
                    authors += ac
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
