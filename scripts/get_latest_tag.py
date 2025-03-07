#!/usr/bin/env python3

import requests
import sys
from datetime import datetime
import argparse

def get_latest_tag(namespace, repository, tag_prefix="", tag_suffix=""):
    """
    Get the latest tag from DockerHub based on update time for given prefix and suffix
    
    Args:
        namespace: DockerHub namespace/organization
        repository: Repository name
        tag_prefix: Prefix of the tag to filter
        tag_suffix: Suffix of the tag to filter
    
    Returns:
        str: Latest matching tag
    """
    url = f"https://hub.docker.com/v2/repositories/{namespace}/{repository}/tags"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        if not data.get('results'):
            print(f"No tags found for {namespace}/{repository}")
            return None
            
        # Filter tags based on prefix and suffix
        matching_tags = []
        for tag_info in data['results']:
            tag_name = tag_info['name']
            # Validate tag format: must be exactly 4 segments when split by '-'
            if tag_name.startswith(tag_prefix) and tag_name.endswith(tag_suffix):
                segments = tag_name.split('-')
                # version-date-commit-arch
                if len(segments) != 4:
                    continue
                # Convert last_updated string to datetime object
                last_updated = datetime.strptime(tag_info['last_updated'], "%Y-%m-%dT%H:%M:%S.%fZ")
                matching_tags.append((tag_name, last_updated))
        
        if not matching_tags:
            print(f"No matching tags found with prefix '{tag_prefix}' and suffix '{tag_suffix}'")
            return None
            
        # Sort by last_updated time and get the most recent one
        latest_tag = sorted(matching_tags, key=lambda x: x[1], reverse=True)[0][0]
        return latest_tag
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching tags: {str(e)}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Get latest Docker image tag with specific prefix and suffix')
    parser.add_argument('-n', '--namespace', required=True, help='DockerHub namespace/organization')
    parser.add_argument('-r', '--repository', required=True, help='Repository name')
    parser.add_argument('-p', '--prefix', default="master", help='Tag prefix to filter')
    parser.add_argument('-a', '--arch', default="amd64", help='Tag arch to filter')
    
    args = parser.parse_args()
    
    latest_tag = get_latest_tag(args.namespace, args.repository, args.prefix, args.arch)
    if latest_tag:
        print(latest_tag)
        return 0
    return 1

if __name__ == "__main__":
    sys.exit(main())
