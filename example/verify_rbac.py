from pymilvus import MilvusClient
import argparse


def main(uri, token):
    client = MilvusClient(
        uri=uri,
        token=token)
    res = client.list_roles()
    print(res)
    assert "roleA" in res
    res = client.list_users()
    print(res)
    assert "user_1" in res


if __name__ == "__main__":
    args = argparse.ArgumentParser(description="prepare data")
    args.add_argument("--uri", type=str, default="http://127.0.0.1:19530", help="Milvus server uri")
    args.add_argument("--token", type=str, default="root:Milvus", help="Milvus server token")
    args = args.parse_args()
    main(args.uri, args.token)

