#!/usr/bin/env python3
import requests
import time
import sys
import argparse

def check_embedding_service(url, max_retries=30, retry_interval=2):
    """
    Check if the Text Embedding Inference service is running and responding to API requests.
    
    Args:
        url (str): The URL of the embedding service endpoint
        max_retries (int): Maximum number of retry attempts
        retry_interval (int): Time in seconds between retry attempts
        
    Returns:
        bool: True if service is running successfully, False otherwise
    """
    print(f"Waiting for Text Embedding Inference service to be ready at {url}...")
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            payload = {"inputs": "What is Deep Learning?"}
            headers = {"Content-Type": "application/json"}
            
            response = requests.post(url, json=payload, headers=headers, timeout=5)
            print(f"Response: {response.json()}")
            if response.status_code == 200 and len(response.json()) > 0:
                print("Text Embedding Inference service is ready and responding to API requests!")
                return True
            else:
                print(f"Service returned status code {response.status_code}")
        except requests.exceptions.RequestException:
            # Service not available yet or connection error
            pass
            
        print(f"Service not ready yet, retrying in {retry_interval} seconds... (Attempt {retry_count+1}/{max_retries})")
        time.sleep(retry_interval)
        retry_count += 1
        
    print("Failed to validate Text Embedding Inference service after multiple attempts")
    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check if Text Embedding Inference service is running")
    parser.add_argument("--url", default="http://127.0.0.1:80/embed", help="URL of the embedding service endpoint")
    parser.add_argument("--max-retries", type=int, default=30, help="Maximum number of retry attempts")
    parser.add_argument("--retry-interval", type=int, default=2, help="Time in seconds between retry attempts")
    
    args = parser.parse_args()
    
    success = check_embedding_service(args.url, args.max_retries, args.retry_interval)
    sys.exit(0 if success else 1)
