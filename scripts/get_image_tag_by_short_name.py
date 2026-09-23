import requests
import argparse
from tenacity import retry, stop_after_attempt

@retry(stop=stop_after_attempt(7))
def get_image_tag_by_short_name(repository, tag, arch):
    
    splits = tag.split("-")
    prefix = f"{splits[0]}-" if len(splits) > 1 else "v"
    url = f"https://hub.docker.com/v2/repositories/{repository}/tags?name={prefix}&ordering=last_updated"
    response = requests.get(url)
    data = response.json()

    # Get the latest tag with the same arch and prefix
    sorted_images = sorted(data["results"], key=lambda x: x["last_updated"], reverse=True)
    # print(sorted_images)
    # get the latest tag with the same arch and prefix
    for tag_info in sorted_images:
        # print(tag_info)
        image_name = tag_info["name"]
        if arch in image_name and len(image_name.split("-"))==4:
            return tag_info["name"]
    # if no tag with the same arch and prefix, ignore arch and return the latest tag
    for tag_info in sorted_images:
        image_name = tag_info["name"]
        if len(image_name.split("-"))==3:
            return tag_info["name"]
    # if no tag with the same prefix, return the short name tag directly
    return tag


if __name__ == "__main__":
    argparse = argparse.ArgumentParser()
    argparse.add_argument("--repository", type=str, default="milvusdb/milvus")
    argparse.add_argument("--tag", type=str, default="master-latest")
    argparse.add_argument("--arch", type=str, default="amd64")
    args = argparse.parse_args()
    if "latest" not in args.tag:
        print(args.tag)
    else:
        res = get_image_tag_by_short_name(args.repository, args.tag, args.arch)
        print(res)
