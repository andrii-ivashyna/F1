import requests
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from requests.exceptions import RequestException, Timeout
import urllib.parse # Added for URL construction

# Assuming BASE_URL, USER_AGENT, REQUEST_TIMEOUT, RATE_LIMIT_DELAY are defined elsewhere
# from .config import BASE_URL, USER_AGENT, REQUEST_TIMEOUT, RATE_LIMIT_DELAY

logger = logging.getLogger(__name__)

class OpenF1APIClient:
    def __init__(self, db_manager, base_url="https://api.openf1.org/v1", user_agent="OpenF1APIClient/1.0", request_timeout=10, rate_limit_delay=0.2): # Added defaults for standalone running
        self.base_url = base_url
        self.db_manager = db_manager
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
        self.request_timeout = request_timeout
        self.rate_limit_delay = rate_limit_delay
    
    def make_request_with_retry(self, endpoint: str, params: Dict[str, Any] = None) -> List[Dict]:
        """Make API request with single retry after 1 second."""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(2):  # 2 attempts total (initial + 1 retry)
            try:
                if attempt > 0:
                    logger.info(f"Retry attempt for {endpoint} with params {params} after 1s delay")
                    time.sleep(1)
                
                # For debugging, let's see what URL requests would build with these params
                # prepared_request = requests.Request('GET', url, params=params).prepare()
                # logger.debug(f"Attempt {attempt+1}: Requesting URL: {prepared_request.url}")

                response = self.session.get(url, params=params, timeout=self.request_timeout)
                
                if response.status_code in [503, 422, 429]:
                    if response.status_code == 503:
                        logger.warning(f"Service unavailable (503) for {endpoint} with params {params}")
                    elif response.status_code == 422:
                        logger.warning(f"Unprocessable entity (422) for {endpoint} with params {params}")
                    elif response.status_code == 429:
                        logger.warning(f"Rate limited (429) for {endpoint} with params {params}")
                    
                    if attempt == 0:
                        continue
                    else:
                        logger.error(f"Max retries reached for {endpoint} with params {params}")
                        if params and 'session_key' in params: # Ensure params is not None
                            return self._try_date_based_splitting(endpoint, params)
                        return []
                
                response.raise_for_status()
                data = response.json()
                
                logger.info(f"Successfully fetched {len(data)} records from {endpoint} with params {params}")
                
                time.sleep(self.rate_limit_delay)
                
                return data
                
            except Timeout as e:
                logger.warning(f"Timeout error on attempt {attempt + 1} for {endpoint} with params {params}: {e}")
                if attempt == 1:
                    logger.error(f"Max retries reached for timeout on {endpoint} with params {params}")
                    if params and 'session_key' in params: # Ensure params is not None
                        return self._try_date_based_splitting(endpoint, params)
                    return []
                continue
                
            except RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1} for {endpoint} with params {params}: {e}")
                if attempt == 1:
                    logger.error(f"Max retries reached for {endpoint} with params {params}: {e}")
                    if params and 'session_key' in params: # Ensure params is not None
                        return self._try_date_based_splitting(endpoint, params)
                    return []
                continue
                
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error on attempt {attempt + 1} for {endpoint} with params {params}: {e}")
                if attempt == 1:
                    logger.error(f"Max retries reached for JSON decode on {endpoint} with params {params}")
                    if params and 'session_key' in params: # Ensure params is not None
                        return self._try_date_based_splitting(endpoint, params)
                    return []
                continue
        
        return []
    
    def _try_date_based_splitting(self, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
        """Try splitting request by hourly time intervals based on session dates.
        This version manually constructs the query string for date parameters
        if the API requires literal '>' and '<' characters (non-standard)."""
        session_key = params.get('session_key')
        if not session_key:
            logger.error("Cannot split by date without session_key")
            return []
        
        # Get session dates from database
        # Ensure db_manager.get_session_dates returns strings or compatible types
        raw_date_start, raw_date_end = self.db_manager.get_session_dates(session_key)
        
        if not raw_date_start or not raw_date_end:
            logger.error(f"Could not get session dates for session {session_key}")
            return []

        logger.info(f"Attempting date-based splitting for session {session_key} from {raw_date_start} to {raw_date_end}")
        
        try:
            # Parse ISO 8601 dates
            # Ensure raw_date_start and raw_date_end are strings
            date_start_str = str(raw_date_start)
            date_end_str = str(raw_date_end)

            start_dt = datetime.fromisoformat(date_start_str.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(date_end_str.replace('Z', '+00:00'))
            
            # Extend end time by 1 hour when splitting by hours to ensure the last hour is included
            # The loop condition is current_dt < end_dt, so an event at exact end_dt might be missed
            # if not extended or logic adjusted.
            # For date<END_HOUR, if end_dt is 08:00, final next_dt can be 08:00, making date<08:00.
            # If original end_dt is session end, extending ensures full coverage if session ends on the hour.
            extended_end_dt = end_dt + timedelta(hours=1) 
            logger.info(f"Original session end time {end_dt.isoformat()}, extended for splitting to {extended_end_dt.isoformat()}")
            
            all_data = []
            current_dt = start_dt
            hour_count = 0
            
            # Iterate up to the *original* end time if strictly splitting within session.
            # If we want to include data up to the *start* of the hour *after* session_end, use extended_end_dt.
            # Given typical date range queries (>= start, < end), using extended_end_dt is safer for hourly chunks.
            while current_dt < extended_end_dt : # Using extended_end_dt ensures the hour containing original end_dt is processed
                # Ensure next_dt does not exceed the extended_end_dt
                next_dt = min(current_dt + timedelta(hours=1), extended_end_dt)
                # If current_dt >= end_dt (original), we might be fetching beyond session.
                # This logic is for splitting; it assumes data might exist across the fetched range.
                # For precise splitting up to original end_dt:
                # next_dt = min(current_dt + timedelta(hours=1), end_dt)
                # And loop while current_dt < end_dt.
                # However, if end_dt is 10:00:00Z and we split hour by hour, the last chunk should be date>=10:00:00Z&date<11:00:00Z
                # to capture anything in the 10th hour. So using extended_end_dt for loop condition and min() is okay.

                if current_dt >= next_dt: # Should not happen with timedelta(hours=1) unless start_dt >= extended_end_dt
                    break

                date_gte_str = current_dt.isoformat().replace('+00:00', 'Z')
                date_lt_str = next_dt.isoformat().replace('+00:00', 'Z')
                
                hour_count += 1

                # Filter out original 'date', 'date>=', 'date<' keys from params to avoid conflict
                other_api_params = {
                    k: v for k, v in params.items() 
                    if k not in ["date", "date>=", "date<"]
                }

                # Manually construct the date part of the query string.
                # ISO date strings are generally URL-safe as values.
                # The keys 'date>=' and 'date<' are used literally here.
                date_query_segment = f"date>={date_gte_str}&date<{date_lt_str}"

                base_request_url = f"{self.base_url}/{endpoint}"
                final_url_with_params: str

                if other_api_params:
                    # URL-encode other parameters
                    encoded_other_params = urllib.parse.urlencode(other_api_params, doseq=True) # doseq=True for list values
                    final_url_with_params = f"{base_request_url}?{encoded_other_params}&{date_query_segment}"
                else:
                    final_url_with_params = f"{base_request_url}?{date_query_segment}"
                
                logger.info(f"Fetching hour {hour_count} ({date_gte_str} to {date_lt_str}) with URL: {final_url_with_params}")
                
                try:
                    # Make request with the fully constructed URL; params should be None
                    response = self.session.get(final_url_with_params, timeout=self.request_timeout)
                    response.raise_for_status() # Raise HTTPError for bad responses (4XX or 5XX)
                    hour_data = response.json()
                    
                    if hour_data: # Check if list is not empty
                        all_data.extend(hour_data)
                        logger.info(f"Hour {hour_count}: fetched {len(hour_data)} records")
                    else:
                        logger.info(f"Hour {hour_count}: no data found for this interval")
                    
                    time.sleep(self.rate_limit_delay) # Rate limiting
                    
                except Timeout:
                    logger.warning(f"Timeout fetching hour {hour_count} ({date_gte_str} to {date_lt_str})")
                except RequestException as e: # Catches HTTPError, ConnectionError, etc.
                    logger.warning(f"Failed to fetch hour {hour_count} ({date_gte_str} to {date_lt_str}): {e}")
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode error for hour {hour_count} ({date_gte_str} to {date_lt_str}): {e}. Response text: {response.text[:200] if response else 'N/A'}")

                current_dt = next_dt
            
            logger.info(f"Date-based splitting completed: {len(all_data)} total records fetched across {hour_count} hourly attempts.")
            return all_data
            
        except Exception as e: # Catch any other errors during date parsing or loop setup
            logger.error(f"Error in date-based splitting setup or execution: {e}", exc_info=True)
            return []

    def make_chunked_request(self, endpoint: str, base_params: Dict[str, Any], 
                           chunk_key: str, chunk_values: List[Any]) -> List[Dict]:
        """Make requests for each value in chunk_values individually."""
        all_data = []
        
        for value in chunk_values:
            params = base_params.copy() # Start with a copy of base parameters
            params[chunk_key] = value   # Add/overwrite the chunk-specific parameter
            
            logger.info(f"Fetching {endpoint} for {chunk_key}={value} with params {params}")
            
            # Use the main make_request_with_retry method which includes retry and date splitting logic
            data = self.make_request_with_retry(endpoint, params) 
            
            if data: # If data is a non-empty list
                all_data.extend(data)
                logger.info(f"Successfully fetched {len(data)} records for {chunk_key}={value}")
            elif isinstance(data, list) and not data: # Empty list, means no data or max retries failed cleanly
                logger.info(f"No data returned or fetched for {chunk_key}={value}")
            # make_request_with_retry returns [] on failure, so no explicit 'else' for failure here is needed
            # as it's covered by the log messages within make_request_with_retry or _try_date_based_splitting.
        
        logger.info(f"Chunked request for {endpoint} on key '{chunk_key}' completed. Total records: {len(all_data)}")
        return all_data
