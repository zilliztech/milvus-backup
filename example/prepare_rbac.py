from pymilvus import MilvusClient
import argparse


def main(uri, token):
    client = MilvusClient(
        uri=uri,
        token=token)

    client.create_user(
        user_name='user_1',
        password='P@ssw0rd'
    )
    client.update_password(
        user_name='user_1',
        old_password='P@ssw0rd',
        new_password='P@ssw0rd123'
    )
    client.create_role(
        role_name="roleA",
    )

    client.grant_privilege(
        role_name='roleA',
        object_type='User',
        object_name='user_1',
        privilege='SelectUser'
    )
    res = client.list_roles()
    print(res)

if __name__ == "__main__":
    args = argparse.ArgumentParser(description="prepare data")
    args.add_argument("--uri", type=str, default="http://127.0.0.1:19530", help="Milvus server uri")
    args.add_argument("--token", type=str, default="root:Milvus", help="Milvus server token")
    args = args.parse_args()
    main(args.uri, args.token)

