#!/usr/bin/env python3

import datetime
import sys
import os
import re
import json
import requests
import traceback
import singer
from singer import Transformer, utils, metadata, metrics
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from dataclasses import dataclass
from typing import Any

import pytz
import backoff
from strict_rfc3339 import rfc3339_to_timestamp
from dateutil.parser import isoparse
from dateutil.tz import tzutc
import psutil
import gc
import asyncio
from urllib.parse import urlparse

from minware_singer_utils import GitLocal, SecureLogger

PER_PAGE_MAX = 100
CONFIG = {
    'api_url': "https://gitlab.com/api/v4",
    'private_token': None,
    'start_date': None,
    'groups': '',
    'ultimate_license': False,
    'fetch_merge_request_commits': False,
    'fetch_merge_request_notes': False,
    'fetch_pipelines_extended': False,
    'fetch_group_variables': False,
    'fetch_project_variables': False,
}
STATE = {}
CATALOG = None

@dataclass
class GitlabResponse:
    response: requests.Response
    json_data: Any

    @property
    def status_code(self) -> int:
        return self.response.status_code

    @property
    def headers(self):
        return self.response.headers

def parse_datetime(datetime_str):
    dt = isoparse(datetime_str)
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

RESOURCES = {
    'projects': {
        'url': '/projects/{id}?statistics=1',
        'schema': load_schema('projects'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_activity_at'],
    },
    'repositories': {
        'schema': load_schema('repositories'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'branches': {
        'url': '/projects/{id}/repository/branches',
        'schema': load_schema('branches'),
        'key_properties': ['project_id', 'name'],
        'replication_method': 'FULL_TABLE',
    },
    'commits': {
        # Not sure what all=true does, but we do want all the commits
        # It's fine for ref_name to be blank, which will fetch the main branch. It can also be set
        # to a commit hash rather than a named ref and works properly.
        # WARNING: don't add all=true here because that seems to reverse the commit order, which can
        # lead to skipping commits.
        'url': '/projects/{id}/repository/commits?since={start_date}&with_stats=true'
            '&ref_name={ref_name}',
        'schema': load_schema('commits'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['created_at'],
    },
    'commit_files': {
        'url': '',
        'schema': load_schema('commit_files'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
    },
    'commit_files_meta': {
        'url': '',
        'schema': load_schema('commit_files_meta'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
    },
    'refs': {
        'url': '',
        'schema': load_schema('refs'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
    },
    'issues': {
        'url': '/projects/{id}/issues?scope=all&updated_after={start_date}',
        'schema': load_schema('issues'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
    },
    'jobs': {
        'url': '/projects/{id}/pipelines/{secondary_id}/jobs?include_retried=true',
        'schema': load_schema('jobs'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'merge_requests': {
        'url': '/projects/{id}/merge_requests?scope=all&updated_after={start_date}',
        'schema': load_schema('merge_requests'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
    },
    'merge_request_commits': {
        'url': '/projects/{id}/merge_requests/{secondary_id}/commits',
        'schema': load_schema('merge_request_commits'),
        'key_properties': ['project_id', 'merge_request_iid', 'commit_id'],
        'replication_method': 'FULL_TABLE',
    },
    'merge_request_notes': {
        'url': '/projects/{id}/merge_requests/{secondary_id}/notes',
        'schema': load_schema('merge_request_notes'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'project_milestones': {
        'url': '/projects/{id}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'group_subgroups': {
        'url': '/groups/{id}/subgroups',
        'schema': load_schema('groups'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'group_milestones': {
        'url': '/groups/{id}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'users': {
        'url': '/projects/{id}/users',
        'schema': load_schema('users'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'site_users': {
        'url': '/users',
        'schema': load_schema('users'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'groups': {
        'url': '/groups/{id}',
        'schema': load_schema('groups'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'project_members': {
        'url': '/projects/{id}/members',
        'schema': load_schema('project_members'),
        'key_properties': ['project_id', 'id'],
        'replication_method': 'FULL_TABLE',
    },
    'group_members': {
        'url': '/groups/{id}/members',
        'schema': load_schema('group_members'),
        'key_properties': ['group_id', 'id'],
        'replication_method': 'FULL_TABLE',
    },
    'group_projects': {
        'url': '/groups/{id}/projects',
        'schema': load_schema('group_projects'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'releases': {
        'url': '/projects/{id}/releases',
        'schema': load_schema('releases'),
        'key_properties': ['project_id', 'commit_id', 'tag_name'],
        'replication_method': 'FULL_TABLE',
    },
    'tags': {
        'url': '/projects/{id}/repository/tags',
        'schema': load_schema('tags'),
        'key_properties': ['project_id', 'commit_id', 'name'],
        'replication_method': 'FULL_TABLE',
    },
    'project_labels': {
        'url': '/projects/{id}/labels',
        'schema': load_schema('project_labels'),
        'key_properties': ['project_id', 'id'],
        'replication_method': 'FULL_TABLE',
    },
    'group_labels': {
        'url': '/groups/{id}/labels',
        'schema': load_schema('group_labels'),
        'key_properties': ['group_id', 'id'],
        'replication_method': 'FULL_TABLE',
    },
    'epics': {
        'url': '/groups/{id}/epics?updated_after={start_date}',
        'schema': load_schema('epics'),
        'key_properties': ['group_id', 'id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
    },
    'epic_issues': {
        'url': '/groups/{id}/epics/{secondary_id}/issues',
        'schema': load_schema('epic_issues'),
        'key_properties': ['group_id', 'epic_iid', 'epic_issue_id'],
        'replication_method': 'FULL_TABLE',
    },
    'pipelines': {
        'url': '/projects/{id}/pipelines?updated_after={start_date}',
        'schema': load_schema('pipelines'),
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
    },
    'pipelines_extended': {
        'url': '/projects/{id}/pipelines/{secondary_id}',
        'schema': load_schema('pipelines_extended'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'vulnerabilities': {
        'url': '/projects/{id}/vulnerabilities',
        'schema': load_schema('vulnerabilities'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'project_variables': {
        'url': '/projects/{id}/variables',
        'schema': load_schema('project_variables'),
        'key_properties': ['project_id', 'key'],
        'replication_method': 'FULL_TABLE',
    },
    'group_variables': {
        'url': '/groups/{id}/variables',
        'schema': load_schema('group_variables'),
        'key_properties': ['group_id', 'key'],
        'replication_method': 'FULL_TABLE',
    }
}

ULTIMATE_RESOURCES = ("epics", "epic_issues")
STREAM_CONFIG_SWITCHES = (
    'merge_request_commits',
    'merge_request_notes',
    'pipelines_extended',
    'group_variables',
    'project_variables',
)

LOGGER = SecureLogger(singer.get_logger())
SESSION = requests.Session()

TRUTHY = ("true", "1", "yes", "on")

class ResourceInaccessible(Exception):
    """
    Base exception for Resources the current user can not access.
    e.g. Unauthorized, Forbidden, Not Found errors
    """

def truthy(val) -> bool:
    return str(val).lower() in TRUTHY

def get_url(entity, id, secondary_id=None, start_date=None, ref_name=None):
    if not isinstance(id, int):
        id = id.replace("/", "%2F")

    if secondary_id and not isinstance(secondary_id, int):
        secondary_id = secondary_id.replace("/", "%2F")

    url = CONFIG['api_url'] + RESOURCES[entity]['url'].format(
            id=id,
            secondary_id=secondary_id,
            start_date=start_date,
            ref_name=ref_name
        )
    LOGGER.info('Beginning sync of entity {}, URL stream {}'.format(entity, url))
    return url


def get_start(entity):
    if entity not in STATE or parse_datetime(STATE[entity]) < parse_datetime(CONFIG['start_date']):
        dates_to_compare = [
            parse_datetime(CONFIG['start_date']),
            datetime.datetime(1980, 1, 1, 0, 0, 0, 0, tzutc())
        ]
        STATE[entity] = max(dates_to_compare).isoformat()
    return STATE[entity]


latest_response = None
latest_request = None

@backoff.on_predicate(backoff.runtime,
                      predicate=lambda r: r.status_code == 429,
                      max_tries=5,
                      value=lambda r: int(r.headers.get("Retry-After")),
                      jitter=None,
                      logger=LOGGER)
@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException, requests.exceptions.JSONDecodeError),
                      max_tries=5,
                      giveup=lambda e: (hasattr(e, 'response') and e.response is not None and e.response.status_code != 429 and 400 <= e.response.status_code < 500),  # hasattr check needed since JSONDecodeError has no response
                      factor=2,
                      logger=LOGGER)
def request(url, params=None) -> GitlabResponse:
    global latest_response
    global latest_request
    params = params or {}

    headers = { "Private-Token": CONFIG['private_token'] }
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    proxies = None if urlparse(url).hostname.endswith('gitlab.com') else {
        'http': os.getenv('MINWARE_PROXY', ''),
        'https': os.getenv('MINWARE_PROXY', '')
    }
    latest_request = { 'method': 'GET', 'url': url, 'params': params}
    resp = SESSION.request('GET', url, params=params, headers=headers, proxies=proxies)
    latest_response = resp
    
    LOGGER.info("GET {} {}".format(url, params))

    if resp.status_code in [401, 403, 404]:
        LOGGER.info("Skipping request to {}".format(url))
        LOGGER.info("Reason: {} - {}".format(resp.status_code, resp.content))
        raise ResourceInaccessible

    # Parse JSON response - will retry if it fails
    try:
        resp_json = resp.json()
    except requests.exceptions.JSONDecodeError as e:
        LOGGER.warning(f"JSON decode error: {str(e)}")
        LOGGER.warning(f"Response status code: {resp.status_code}")
        LOGGER.warning(f"Response text: {resp.text}")
        raise  # Let backoff retry

    return GitlabResponse(response=resp, json_data=resp_json)

def gen_request(url):
    if 'labels' in url:
        # The labels API is timing out for large per_page values
        #  https://gitlab.com/gitlab-org/gitlab-ce/issues/63103
        # Keeping it at 20 until the bug is fixed
        per_page = 20
    else:
        per_page = PER_PAGE_MAX

    params = {
        'page': 1,
        'per_page': per_page
    }

    # X-Total-Pages header is not always available since GitLab 11.8
    #  https://docs.gitlab.com/ee/api/#other-pagination-headers
    # X-Next-Page to check if there is another page available and iterate
    next_page = 1

    try:
        while next_page:
            params['page'] = int(next_page)
            gitlab_resp = request(url, params)
            # handle endpoints that return a single JSON object
            if isinstance(gitlab_resp.json_data, dict):
                yield gitlab_resp.json_data
            # handle endpoints that return an array of JSON objects
            else:
                for row in gitlab_resp.json_data:
                    yield row
            next_page = gitlab_resp.headers.get('X-Next-Page', None)
    except ResourceInaccessible as exc:
        # Don't halt execution if a Resource is Inaccessible
        # Just skip it and continue with the rest of the extraction
        return []

def format_timestamp(data, typ, schema):
    result = data
    if data and typ == 'string' and schema.get('format') == 'date-time':
        match = re.match(r'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) ([-+]\d{2})(\d{2})', data)
        if match:
            data = '{}T{}{}:{}'.format(match.group(1), match.group(2), match.group(3), match.group(4))
        rfc3339_ts = rfc3339_to_timestamp(data)
        utc_dt = datetime.datetime.utcfromtimestamp(rfc3339_ts).replace(tzinfo=pytz.UTC)
        result = utils.strftime(utc_dt)

    return result

def flatten_id(item, target):
    if target in item and item[target] is not None:
        item[target + '_id'] = item.pop(target, {}).pop('id', None)
    else:
        item[target + '_id'] = None

def sync_branches(project, headsOnly=False):
    entity = "branches"
    stream = CATALOG.get_stream(entity)
    if not headsOnly and (stream is None or not stream.is_selected()):
        return {}
    if not headsOnly:
        mdata = metadata.to_map(stream.metadata)

    # Return all of the branch commit shas
    heads = {}

    url = get_url(entity="branches", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            heads['refs/heads/' + row['name']] = row['commit']['id']
            if headsOnly:
                continue
            row['project_id'] = project['id']
            flatten_id(row, "commit")
            transformed_row = transformer.transform(row, RESOURCES["branches"]["schema"], mdata)
            singer.write_record("branches", transformed_row, time_extracted=utils.now())

    # No state here -- always return all branches

    return heads

def sync_commits(project, heads):
    entity = "commits"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    state_key = "project_{}_commits".format(project["id"])
    start_date=get_start(state_key)
    if not start_date:
        # Don't use 1970, it leads to empty results for some reason.
        start_date = '1980-01-01'

    # Keep a state for the commits fetched per project
    state_key_fetchedCommits = state_key + '_fetchedCommits'
    fetchedCommits = STATE[state_key_fetchedCommits] if state_key_fetchedCommits in STATE else None
    if not fetchedCommits:
        fetchedCommits = {}
    else:
        # We have run previously, so we don't want to use the time-based bookmark becuase it could
        # skip commits that are pushed after they are committed. So, reset the 'since' bookmark back
        # to the beginning of time and rely solely on the fetchedCommits bookmark.
        start_date = '1980-01-01'

        # Don't allow any intermediate changes to alter the state
        fetchedCommits = fetchedCommits.copy()

    for headRef in heads:
        head = heads[headRef]
        # If the head commit has already been synced, then skip.
        if head in fetchedCommits:
            LOGGER.info('skipping head commit {}'.format(head))
            continue
        LOGGER.info('loading head commit {}'.format(head))

        # Maintain a list of parents we are waiting to see
        missingParents = {}

        url = get_url(entity=entity, id=project['id'], start_date=start_date, ref_name=head)
        with Transformer(pre_hook=format_timestamp) as transformer:
            for row in gen_request(url):
                # Skip commits we've already imported
                if row['id'] in fetchedCommits:
                    continue

                # Record that we have now fetched this commit
                fetchedCommits[row['id']] = 1
                # No longer a missing parent
                missingParents.pop(row['id'], None)

                # Keep track of new missing parents
                for parent_sha in row['parent_ids']:
                    if not parent_sha in fetchedCommits:
                        missingParents[parent_sha] = 1

                row['project_id'] = project["id"]
                transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

                singer.write_record(entity, transformed_row, time_extracted=utils.now())
                utils.update_state(STATE, state_key, row['created_at'])

                # If there are no missing parents, then we are done prior to reaching the last page
                if not missingParents:
                    break

            if missingParents:
                raise Exception('Some commit parents never found: ' +
                    ','.join(missingParents.keys()))

    STATE[state_key_fetchedCommits] = fetchedCommits

    singer.write_state(STATE)


def get_commit_detail_local(commit, repo_path, gitLocal):
    try:
        changes = gitLocal.getCommitDiff(repo_path, commit['sha'])
        commit['files'] = changes
    except Exception as e:
        # This generally shouldn't happen since we've already fetched and checked out the head
        # commit successfully, so it probably indicates some sort of system error. Just let it
        # bubble up for now.
        raise e

def get_commit_changes(commit, repo_path, useLocal, gitLocal):
    get_commit_detail_local(commit, repo_path, gitLocal)
    commit['_sdc_repository'] = repo_path
    commit['id'] = '{}/{}'.format(repo_path, commit['sha'])
    return commit

async def getChangedfilesForCommits(commits, repo_path, hasLocal, gitLocal):
    coros = []
    for commit in commits:
        changesCoro = asyncio.to_thread(get_commit_changes, commit, repo_path, hasLocal, gitLocal)
        coros.append(changesCoro)
    results = await asyncio.gather(*coros)
    return results

def process_head_commits(gitLocal, repo_path, headRef, headSha, fetchedCommits, commitQ, LOG_PAGE_SIZE):
    """
    Process all commits for a given head, handling pagination and parent tracking.
    Returns the newlyFetchedCommits dictionary.
    """
    missingParents = {}
    offset = 0
    newlyFetchedCommits = {}
    
    while True:
        # Get commits one page at a time
        commits = gitLocal.getCommitsFromHeadPyGit(repo_path, headSha,
            limit = LOG_PAGE_SIZE, offset = offset, skipAtCommits=fetchedCommits)
        
        for commit in commits:
            # Skip commits we've already imported
            if commit['sha'] in fetchedCommits or commit['sha'] in newlyFetchedCommits:
                continue

            commitQ.append(commit)

            # Record that we have now fetched this commit
            newlyFetchedCommits[commit['sha']] = 1
            # No longer a missing parent
            missingParents.pop(commit['sha'], None)

            # Keep track of new missing parents
            for parent in commit['parents']:
                if not parent['sha'] in fetchedCommits and not parent['sha'] in newlyFetchedCommits:
                    missingParents[parent['sha']] = 1

        # If there are no missing parents, then we are done prior to reaching the last page
        if not missingParents:
            break
        elif len(commits) > 0:
            offset += LOG_PAGE_SIZE
        # Else if we have reached the end of our data but not found the parents, then we
        # have a problem
        else:
            raise Exception('Some commit parents never found for {}: {}'.format(
                headRef, ','.join(missingParents.keys())))
    
    return newlyFetchedCommits

def fetch_missing_refs_batch(gitLocal, repo_path, missing_refs):
    """
    Attempt to fetch all missing refs in a single batch operation.
    Returns list of refs that should be processed (empty list if fetch failed).
    """
    if not missing_refs:
        return []
    
    # Extract just the SHAs for the batch fetch
    missing_shas = [ref_info['sha'] for ref_info in missing_refs]
    
    LOGGER.info('Attempting to batch fetch {} missing refs for {}'.format(len(missing_shas), repo_path))
    
    try:
        # Call the GitLocal batch fetch method from minware-singer-utils PR #12
        gitLocal.fetchMultipleCommits(repo_path, missing_shas, 'gitlab')
        LOGGER.info('Successfully batch fetched {} refs for {}'.format(len(missing_shas), repo_path))
        return missing_refs  # Return all refs for processing
            
    except Exception as e:
        LOGGER.warning('Batch fetch failed with exception for {}: {}'.format(repo_path, str(e)))
        return []


def sync_commit_files(project, heads, gitLocal, commits_only=False, selected_stream_ids=None):
    entity = 'commit_files_meta' if commits_only else 'commit_files'
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    refentity = 'refs'
    refstream = CATALOG.get_stream(refentity)
    refs_selected = refstream is not None and refstream.is_selected()
    if refs_selected:
        refmdata = metadata.to_map(refstream.metadata)

    # Keep a state for the commits fetched per project
    state_key = "project_{}_{}".format(project["id"], entity)
    fetchedCommits = STATE[state_key] if state_key in STATE else None
    if not fetchedCommits:
        fetchedCommits = {}
    else:
        # Don't allow any intermediate changes to alter the state
        fetchedCommits = fetchedCommits.copy()

    # Get all of the branch heads to use for querying commits
    #heads = get_all_heads_for_commits(repo_path)
    repo_path = project['path_with_namespace']
    #heads = gitLocal.getAllHeads(repo_path, 'gitlab')

    # Set this here for updating the state when we don't run any queries
    extraction_time = singer.utils.now()

    count = 0
    # The large majority of PRs are less than this many commits
    LOG_PAGE_SIZE = 100

    # First, walk through all the heads and queue up all the commits that need to be imported
    commitQ = []
    missing_refs = []  # Track missing refs for batch fetch

    for headRef in heads:
        count += 1
        if count % 10 == 0:
            process = psutil.Process(os.getpid())
            LOGGER.info('Processed heads {}/{}, {} bytes'.format(count, len(heads),
                process.memory_info().rss))
        headSha = heads[headRef]
        # If the head commit has already been synced, then skip.
        if headSha in fetchedCommits:
            continue

        if headSha is None:
            LOGGER.warn('sha for {} does not exist, skipping'.format(headRef))
            continue

        # Emit the ref record as well if it's not for a pull request (only if refs stream is selected)
        if refs_selected and not ('refs/pull' in headRef) and selected_stream_ids and 'refs' in selected_stream_ids:
            refRecord = {
                'id': '{}/{}'.format(repo_path, headRef),
                '_sdc_repository': repo_path,
                'ref': headRef,
                'sha': headSha
            }
            with Transformer(pre_hook=format_timestamp) as transformer:
                rec = transformer.transform(refRecord, RESOURCES[refentity]["schema"], refmdata)
            singer.write_record('refs', rec, time_extracted=extraction_time)

        # Verify that this commit exists in our mirrored repo
        hasLocal = gitLocal.hasLocalCommit(repo_path, headSha, 'gitlab')
        if not hasLocal:
            LOGGER.debug('MISSING REF/COMMIT {}/{}/{} - will attempt batch fetch'.format(repo_path, headRef, headSha))
            missing_refs.append({'ref': headRef, 'sha': headSha})
            continue

        # Process all commits for this head
        extraction_time = singer.utils.now()
        newlyFetchedCommits = process_head_commits(gitLocal, repo_path, headRef, headSha, 
                                                  fetchedCommits, commitQ, LOG_PAGE_SIZE)
        
        # After successfully processing all commits for this head, add them to fetchedCommits
        fetchedCommits.update(newlyFetchedCommits)

    # Attempt to batch fetch any missing refs before processing commits
    if missing_refs:
        LOGGER.info('Found {} missing refs, attempting batch fetch for {}'.format(len(missing_refs), repo_path))
        fetched_refs = fetch_missing_refs_batch(gitLocal, repo_path, missing_refs)
        
        if fetched_refs:
            LOGGER.info('Batch fetch succeeded, processing {} refs'.format(len(fetched_refs)))
            
            # Process the fetched refs
            for ref_info in fetched_refs:
                headRef = ref_info['ref']
                headSha = ref_info['sha']
                
                LOGGER.debug('Processing batch-fetched ref {}/{}'.format(headRef, headSha))
                
                # Process this head's commits using the extracted function
                newlyFetchedCommits = process_head_commits(gitLocal, repo_path, headRef, headSha,
                                                          fetchedCommits, commitQ, LOG_PAGE_SIZE)
                
                fetchedCommits.update(newlyFetchedCommits)
        else:
            LOGGER.warning('Batch fetch failed for {} refs in {}'.format(len(missing_refs), repo_path))

    # Now run through all the commits in parallel
    gc.collect()
    process = psutil.Process(os.getpid())
    LOGGER.info('Processing {} commits, mem(mb) {}'.format(len(commitQ),
        process.memory_info().rss / (1024 * 1024)))

    # Run in batches
    i = 0
    BATCH_SIZE = 16
    PRINT_INTERVAL = 16
    hasLocal = True # Only local now
    totalCommits = len(commitQ)
    finishedCount = 0

    while len(commitQ) > 0:
        # Slice off the queue to avoid memory leaks
        curQ = commitQ[0:BATCH_SIZE]
        commitQ = commitQ[BATCH_SIZE:]
        if commits_only:
            for commit in curQ:
                commit['files'] = []
                commit['_sdc_repository'] = repo_path
                commit['id'] = '{}/{}'.format(repo_path, commit['sha'])
            changedFileList = curQ
        else:
            changedFileList = asyncio.run(getChangedfilesForCommits(curQ, repo_path, hasLocal,
                gitLocal))
        for commitfiles in changedFileList:
            with Transformer(pre_hook=format_timestamp) as transformer:
                rec = transformer.transform(commitfiles, RESOURCES[entity]["schema"], mdata)
            singer.write_record(entity, rec, time_extracted=extraction_time)

        finishedCount += BATCH_SIZE
        if i % (BATCH_SIZE * PRINT_INTERVAL) == 0:
            curQ = None
            changedFileList = None
            gc.collect()
            process = psutil.Process(os.getpid())
            LOGGER.info('Imported {}/{} commits, {}/{} MB'.format(finishedCount, totalCommits,
                process.memory_info().rss / (1024 * 1024),
                process.memory_info().data / (1024 * 1024)))


    # Don't write until the end so that we don't record fetchedCommits if we fail and never get
    # their parents.
    STATE[state_key] = fetchedCommits

    # Just rely on the sync state call at the end so we don't emit duplicate states at the end,
    # which can be large.
    #singer.write_state(STATE)

def sync_issues(project):
    entity = "issues"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    # Keep a state for the issues fetched per project
    state_key = "project_{}_issues".format(project["id"])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=project['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            flatten_id(row, "assignee")
            flatten_id(row, "epic")
            flatten_id(row, "closed_by")
            flatten_id(row, "milestone")

            # Get the assignee ids
            assignee_ids = []
            for assignee in row.get("assignees"):
                assignee_ids.append(assignee["id"])
            row["assignees"] = assignee_ids

            # Get the time_stats
            time_stats = row.get("time_stats")
            if time_stats:
                row["time_estimate"] = time_stats.get("time_estimate")
                row["total_time_spent"] = time_stats.get("total_time_spent")
                row["human_time_estimate"] = time_stats.get("human_time_estimate")
                row["human_total_time_spent"] = time_stats.get("human_total_time_spent")
            else:
                row["time_estimate"] = None
                row["total_time_spent"] = None
                row["human_time_estimate"] = None
                row["human_total_time_spent"] = None

            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

    singer.write_state(STATE)

def sync_merge_requests(project, headsOnly=False):
    entity = "merge_requests"
    stream = CATALOG.get_stream(entity)
    if not headsOnly and (stream is None or not stream.is_selected()):
        return {}
    if not headsOnly:
        mdata = metadata.to_map(stream.metadata)

    # Keep a state for the merge requests fetched per project
    state_key = "project_{}_merge_requests".format(project["id"])
    start_date=get_start(state_key)

    heads = {}

    url = get_url(entity=entity, id=project['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            heads['refs/pull/{}/head'.format(row['iid'])] = row['sha']
            if headsOnly:
                utils.update_state(STATE, state_key, row['updated_at'])
                continue
            # Include the full info rather than just the ID
            #flatten_id(row, "author")
            #flatten_id(row, "assignee")
            flatten_id(row, "milestone")
            #flatten_id(row, "merged_by")
            #flatten_id(row, "closed_by")

            # Get the assignee ids
            assignee_ids = []
            for assignee in row.get("assignees"):
                assignee_ids.append(assignee["id"])
            row["assignees"] = assignee_ids

            # Get the reviewer ids
            reviewer_ids = []
            for reviewer in row.get("reviewers"):
                reviewer_ids.append(reviewer["id"])
            row["reviewers"] = reviewer_ids

            # Get the time_stats
            time_stats = row.get("time_stats")
            if time_stats:
                row["time_estimate"] = time_stats.get("time_estimate")
                row["total_time_spent"] = time_stats.get("total_time_spent")
                row["human_time_estimate"] = time_stats.get("human_time_estimate")
                row["human_total_time_spent"] = time_stats.get("human_total_time_spent")
            else:
                row["time_estimate"] = None
                row["total_time_spent"] = None
                row["human_time_estimate"] = None
                row["human_total_time_spent"] = None

            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            # Write the MR record
            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

            # And then sync all the commits for this MR
            # (if it has changed, new commits may be there to fetch)
            sync_merge_request_commits(project, transformed_row)

            # And then sync all the commits for this MR
            # (if it has changed, new commits may be there to fetch)
            sync_merge_request_notes(project, transformed_row)

    # Do not write state until subsequent commits have been imported, since those depend on the
    # list of PR head SHAs.
    #singer.write_state(STATE)

    return heads

def sync_merge_request_commits(project, merge_request):
    # This stream will be disabled in favor of directly importing the commits
    entity = "merge_request_commits"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=entity, id=project['id'], secondary_id=merge_request['iid'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            row['merge_request_iid'] = merge_request['iid']
            row['commit_id'] = row['id']
            row['commit_short_id'] = row['short_id']
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            singer.write_record(entity, transformed_row, time_extracted=utils.now())

def sync_merge_request_notes(project, merge_request):
    entity = "merge_request_notes"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=entity, id=project['id'], secondary_id=merge_request['iid'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['project_id'] = project['id']
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            singer.write_record(entity, transformed_row, time_extracted=utils.now())

def sync_releases(project):
    entity = "releases"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="releases", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            flatten_id(row, "commit")
            row['project_id'] = project["id"]
            transformed_row = transformer.transform(row, RESOURCES["releases"]["schema"], mdata)

            singer.write_record("releases", transformed_row, time_extracted=utils.now())


def sync_tags(project):
    entity = "tags"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="tags", id=project['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "commit")
            row['project_id'] = project["id"]
            transformed_row = transformer.transform(row, RESOURCES["tags"]["schema"], mdata)

            singer.write_record("tags", transformed_row, time_extracted=utils.now())


def sync_milestones(entity, element="project"):
    stream_name = "{}_milestones".format(element)
    stream = CATALOG.get_stream(stream_name)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=element + "_milestones", id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES[element + "_milestones"]["schema"], mdata)

            singer.write_record(element + "_milestones", transformed_row, time_extracted=utils.now())

def sync_users(project):
    entity = "users"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="users", id=project['id'])
    project["users"] = []
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES["users"]["schema"], mdata)
            project["users"].append(row["id"])
            singer.write_record("users", transformed_row, time_extracted=utils.now())

def sync_site_users():
    entity = "site_users"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="site_users", id="all")
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES["users"]["schema"], mdata)
            singer.write_record("site_users", transformed_row, time_extracted=utils.now())


def sync_members(entity, element="project"):
    stream_name = "{}_members".format(element)
    member_stream = CATALOG.get_stream(stream_name)
    if member_stream is None or not member_stream.is_selected():
        return
    user_stream = CATALOG.get_stream('users')
    member_mdata = metadata.to_map(member_stream.metadata)
    user_mdata = metadata.to_map(user_stream.metadata)

    url = get_url(entity=stream_name, id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            # First, write a record for the user
            if user_stream.is_selected():
                user_row = transformer.transform(row, RESOURCES["users"]["schema"], user_mdata)
                singer.write_record("users", user_row, time_extracted=utils.now())

            # And then a record for the member
            row[element + '_id'] = entity['id']
            row['user_id'] = row['id']
            member_row = transformer.transform(row, RESOURCES[element + "_members"]["schema"], member_mdata)
            singer.write_record(element + "_members", member_row, time_extracted=utils.now())


def sync_labels(entity, element="project"):
    stream_name = "{}_labels".format(element)
    stream = CATALOG.get_stream(stream_name)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=element + "_labels", id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row[element + '_id'] = entity['id']
            transformed_row = transformer.transform(row, RESOURCES[element + "_labels"]["schema"], mdata)
            singer.write_record(element + "_labels", transformed_row, time_extracted=utils.now())

def sync_epic_issues(group, epic):
    entity = "epic_issues"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="epic_issues", id=group['id'], secondary_id=epic['iid'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row['group_id'] = group['id']
            row['epic_iid'] = epic['iid']
            row['issue_id'] = row['id']
            row['issue_iid'] = row['iid']
            transformed_row = transformer.transform(row, RESOURCES["epic_issues"]["schema"], mdata)

            singer.write_record("epic_issues", transformed_row, time_extracted=utils.now())

def sync_epics(group):
    entity = "epics"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    # Keep a state for the epics fetched per group
    state_key = "group_{}_epics".format(group['id'])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=group['id'], start_date=start_date)
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            flatten_id(row, "author")
            transformed_row = transformer.transform(row, RESOURCES[entity]["schema"], mdata)

            # Write the Epic record
            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

            # And then sync all the issues for that Epic
            # (if it has changed, new issues may be there to fetch)
            sync_epic_issues(group, transformed_row)

    singer.write_state(STATE)

def sync_group(gid, pids, gitLocal, commits_only, selected_stream_ids=None):
    stream = CATALOG.get_stream("groups")
    mdata = metadata.to_map(stream.metadata)
    url = get_url(entity="groups", id=gid)

    try:
        data = request(url).json_data
    except ResourceInaccessible as exc:
        # Don't halt execution if a Group is Inaccessible
        # Just skip it and continue with the rest of the extraction
        return

    time_extracted = utils.now()

    if not pids:
        #  Get all the projects of the group if none are provided
        group_projects_url = get_url(entity="group_projects", id=gid)
        for project in gen_request(group_projects_url):
            if project["id"]:
                sync_project(project["id"], gitLocal, commits_only, selected_stream_ids)

        group_subgroups_url = get_url("group_subgroups", id=gid)
        for group in gen_request(group_subgroups_url):
            if group['id']:
                sync_group(group['id'], [], gitLocal, commits_only, selected_stream_ids)
    else:
        # Sync only specific projects of the group, if explicit projects are provided
        for pid in pids:
            if pid.startswith(data['full_path'] + '/') or pid in [str(p['id']) for p in data['projects']]:
                sync_project(pid, gitLocal, commits_only, selected_stream_ids)

    sync_milestones(data, "group")

    sync_members(data, "group")

    sync_labels(data, "group")

    sync_variables(data, "group")

    if CONFIG['ultimate_license']:
        sync_epics(data)

    if not stream.is_selected():
        return

    with Transformer(pre_hook=format_timestamp) as transformer:
        group = transformer.transform(data, RESOURCES["groups"]["schema"], mdata)
        singer.write_record("groups", group, time_extracted=time_extracted)

def sync_pipelines(project):
    entity = "pipelines"
    stream = CATALOG.get_stream(entity)

    if stream is None or not stream.is_selected():
        return

    mdata = metadata.to_map(stream.metadata)
    # Keep a state for the pipelines fetched per project
    state_key = "project_{}_pipelines".format(project['id'])
    start_date=get_start(state_key)

    url = get_url(entity=entity, id=project['id'], start_date=start_date)

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):

            pipeline_record = {
                **row,
                'project_id': project['id'],
                '_sdc_repository': project['path_with_namespace']
            }
            transformed_row = transformer.transform(pipeline_record, RESOURCES[entity]["schema"], mdata)

            # Write the Pipeline record
            singer.write_record(entity, transformed_row, time_extracted=utils.now())
            utils.update_state(STATE, state_key, row['updated_at'])

            # Sync additional details of a pipeline using get-a-single-pipeline endpoint
            # https://docs.gitlab.com/ee/api/pipelines.html#get-a-single-pipeline
            sync_pipelines_extended(project, transformed_row)

            # Sync all jobs attached to the pipeline.
            # Although jobs cannot be queried by updated_at, if a job changes
            # it's pipeline's updated_at is changed.
            sync_jobs(project, transformed_row)

    singer.write_state(STATE)

def sync_pipelines_extended(project, pipeline):
    entity = "pipelines_extended"
    stream = CATALOG.get_stream(entity)
    if not stream.is_selected():
        return

    mdata = metadata.to_map(stream.metadata)
    url = get_url(entity=entity, id=project['id'], secondary_id=pipeline['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            pipeline_extended_record = {
                **row,
                'project_id': project['id'],
                '_sdc_repository': project['path_with_namespace']
            }
            transformed_row = transformer.transform(pipeline_extended_record, RESOURCES[entity]["schema"], mdata)

            singer.write_record(entity, transformed_row, time_extracted=utils.now())

def sync_vulnerabilities(project):
    entity = "vulnerabilities"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity="vulnerabilities", id=project['id'])
    project["vulnerabilities"] = []
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            transformed_row = transformer.transform(row, RESOURCES["vulnerabilities"]["schema"], mdata)
            project["vulnerabilities"].append(row["id"])
            singer.write_record("vulnerabilities", transformed_row, time_extracted=utils.now())

def sync_jobs(project, pipeline):
    entity = "jobs"
    stream = CATALOG.get_stream(entity)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=entity, id=project['id'], secondary_id=pipeline['id'])
    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            job_record = {
                **row,
                'project_id': project['id'],
                'pipeline_id': pipeline['id'],
                '_sdc_repository': project['path_with_namespace']
            }
            transformed_row = transformer.transform(job_record, RESOURCES[entity]['schema'], mdata)
            singer.write_record(entity, transformed_row, time_extracted=utils.now())

def write_repository(raw_repo):
    # if we can't see default_branch, we don't have code access
    if not "default_branch" in raw_repo:
        return

    stream = CATALOG.get_stream('repositories')
    if stream is None:
        return

    extraction_time = singer.utils.now()
    org_name, repo_name = raw_repo["path_with_namespace"].split('/', 1)
    repo = {}
    repo['id'] = 'gitlab/{}/{}'.format(org_name, repo_name)
    repo['source'] = 'gitlab'
    repo['org_name'] = org_name
    repo['repo_name'] = repo_name
    repo['is_source_public'] = raw_repo["visibility"] != "private"
    # TODO: handle forks
    repo['fork_org_name'] = None
    repo['fork_repo_name'] = None
    repo['description'] = raw_repo["description"]
    repo['default_branch'] = raw_repo["default_branch"]
    with singer.Transformer() as transformer:
        rec = transformer.transform(repo, Schema.to_dict(stream.schema), metadata=metadata.to_map(stream.metadata))
    singer.write_record('repositories', rec, time_extracted=extraction_time)

def sync_variables(entity, element="project"):
    stream_name = "{}_variables".format(element)
    stream = CATALOG.get_stream(stream_name)
    if stream is None or not stream.is_selected():
        return
    mdata = metadata.to_map(stream.metadata)

    url = get_url(entity=element + "_variables", id=entity['id'])

    with Transformer(pre_hook=format_timestamp) as transformer:
        for row in gen_request(url):
            row[element + '_id'] = entity['id']
            transformed_row = transformer.transform(row, RESOURCES[element + "_variables"]["schema"], mdata)
            singer.write_record(element + "_variables", transformed_row, time_extracted=utils.now())

def sync_project(pid, gitLocal, commits_only, selected_stream_ids=None):
    url = get_url(entity="projects", id=pid)

    try:
        data = request(url).json_data
    except ResourceInaccessible as exc:
        # Don't halt execution if a Project is Inaccessible
        # Just skip it and continue with the rest of the extraction
        return

    write_repository(data)

    time_extracted = utils.now()

    state_key = "project_{}".format(data["id"])

    #pylint: disable=maybe-no-member
    last_activity_at = data.get('last_activity_at', data.get('created_at'))
    if not last_activity_at:
        raise Exception(
            #pylint: disable=line-too-long
            "There is no last_activity_at or created_at field on project {}. This usually means I don't have access to the project."
            .format(data['id']))

    stream = CATALOG.get_stream("projects")
    if stream is not None and stream.is_selected():
        mdata = metadata.to_map(stream.metadata)

        with Transformer(pre_hook=format_timestamp) as transformer:
            flatten_id(data, "owner")
            project = transformer.transform(data, RESOURCES["projects"]["schema"], mdata)
            singer.write_record("projects", project, time_extracted=time_extracted)

        utils.update_state(STATE, state_key, last_activity_at)
        singer.write_state(STATE)

    # If commit_files or commit_files_meta is selected, then skip the other streams
    commitFilesStream = CATALOG.get_stream('commit_files')
    commitFilesMetaStream = CATALOG.get_stream('commit_files_meta')
    if (commitFilesStream is None or not commitFilesStream.is_selected()) and \
       (commitFilesMetaStream is None or not commitFilesMetaStream.is_selected()):
        commitFiles = False
    else:
        commitFiles = True

    if commitFiles:
        heads = sync_branches(data, True)
        # This function will utilize the state so that PR heads won't be returned if they haven't
        # been updated since the last run.
        # NOTE: the reason we have to do this is because git clone --mirror doesn't mirror refs for
        # all the PR heads with gitlab like it does for github.
        pr_heads = sync_merge_requests(data, True)
        heads.update(pr_heads)
        # commits_only is now passed as parameter
        sync_commit_files(data, heads, gitLocal, commits_only, selected_stream_ids)
    elif data['last_activity_at'] >= get_start(state_key):
        sync_members(data)
        sync_users(data)
        sync_issues(data)
        # These will both return a dict of REFNAME => SHA
        heads = sync_branches(data, False)
        pr_heads = sync_merge_requests(data, False)
        heads.update(pr_heads)
        sync_commits(data, heads)
        sync_milestones(data)
        sync_labels(data)
        sync_releases(data)
        sync_tags(data)
        sync_pipelines(data)
        sync_vulnerabilities(data)
        sync_variables(data)


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                    selected_streams.append(stream['tap_stream_id'])
    return selected_streams

def do_sync():
    LOGGER.info("Starting sync")

    gids = list(filter(None, CONFIG['groups'].split(' ')))
    pids = list(filter(None, CONFIG['projects'].split(' ')))

    # Get selected stream IDs and determine commit-only mode
    selected_stream_ids = []
    for stream in CATALOG.get_selected_streams(STATE):
        selected_stream_ids.append(stream.tap_stream_id)
    commits_only = 'commit_files_meta' in selected_stream_ids

    for stream in CATALOG.get_selected_streams(STATE):
        singer.write_schema(stream.tap_stream_id, stream.schema.to_dict(), stream.key_properties)


    domain = CONFIG['pull_domain'] if 'pull_domain' in CONFIG else 'gitlab.com'
    gitLocal = GitLocal({
        'access_token': CONFIG['private_token'],
        'workingDir': '/tmp',
        'proxy': os.environ.get("MINWARE_PROXY") if not domain.endswith('gitlab.com') else None
    }, 'https://oauth2:{}@' + domain + '/{}.git',
        CONFIG['hmac_token'] if 'hmac_token' in CONFIG else None,
        LOGGER,
        commitsOnly=commits_only)

    sync_site_users()

    LOGGER.info(gids)

    for gid in gids:
        sync_group(gid, pids, gitLocal, commits_only, selected_stream_ids)

    if not gids:
        # When not syncing groups
        for pid in pids:
            sync_project(pid, gitLocal, commits_only, selected_stream_ids)

    # Write the final STATE
    # This fixes syncing using groups, which don't emit a STATE message
    #  so the last message is not a STATE message
    #  which, in turn, breaks the behavior of some targets that expect a STATE
    #  as the last message
    # It is also a safeguard for future updates
    singer.write_state(STATE)

    LOGGER.info("Sync complete")

def get_catalog_entry(resource, config):
    api_url_regex = re.compile(r'^gitlab.com')
    mdata = metadata.get_standard_metadata(
        schema=config["schema"],
        key_properties=config["key_properties"],
        replication_method=config["replication_method"],
    )

    if (
        resource in ULTIMATE_RESOURCES and not CONFIG["ultimate_license"]
    ) or (
        resource == "site_users" and api_url_regex.match(CONFIG['api_url']) is not None
    ) or (
        resource in STREAM_CONFIG_SWITCHES and not CONFIG["fetch_{}".format(resource)]
    ):
        mdata = metadata.to_list(metadata.write(metadata.to_map(mdata), (), 'inclusion', 'unsupported'))

    return CatalogEntry(
                tap_stream_id=resource,
                stream=resource,
                schema=Schema.from_dict(config["schema"]),
                key_properties=config["key_properties"],
                metadata=mdata,
                replication_key=config.get("replication_keys"),
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=config["replication_method"],
            )

def get_catalog(raw_catalog):
    streams = []

    for config in raw_catalog["streams"]:
        resource = config["tap_stream_id"]
        streams.append(get_catalog_entry(resource, config))

    return Catalog(streams)

def do_discover(select_all=False):
    streams = []
    for resource, config in RESOURCES.items():
        streams.append(get_catalog_entry(resource, config))
    return Catalog(streams)

def main_impl():
    # TODO: Address properties that are required or not
    args = utils.parse_args(["private_token", "projects"])
    args.config["private_token"] = args.config["private_token"].strip()
    LOGGER.addToken(args.config["private_token"])

    CONFIG.update(args.config)
    CONFIG['ultimate_license'] = truthy(CONFIG['ultimate_license'])
    CONFIG['fetch_merge_request_commits'] = truthy(CONFIG['fetch_merge_request_commits'])
    CONFIG['fetch_merge_request_notes'] = truthy(CONFIG['fetch_merge_request_notes'])
    CONFIG['fetch_pipelines_extended'] = truthy(CONFIG['fetch_pipelines_extended'])
    CONFIG['fetch_group_variables'] = truthy(CONFIG['fetch_group_variables'])
    CONFIG['fetch_project_variables'] = truthy(CONFIG['fetch_project_variables'])


    if '/api/' not in CONFIG['api_url']:
        CONFIG['api_url'] += '/api/v4'

    if args.state:
        STATE.update(args.state)

    # If discover flag was passed, log an info message and exit
    global CATALOG
    if args.discover:
        CATALOG = do_discover()
        CATALOG.dump()
    # Otherwise run in sync mode
    else:
        CATALOG = args.catalog if args.catalog else do_discover(select_all=True)
        do_sync()

def main():
    try:
        main_impl()
    except Exception as exc:
        global latest_response
        global latest_request
        for line in traceback.format_exc().splitlines():
            LOGGER.critical(line)
        if latest_response and latest_request:
            LOGGER.critical('Latest Request URL: {}'.format(latest_request['url']))
            LOGGER.critical('Response Code: {}'.format(latest_response.status_code))
            LOGGER.critical('Response Data:')
            LOGGER.critical(latest_response.text)

        sys.exit(1)


if __name__ == '__main__':
    main()
