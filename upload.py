from minio import Minio
from minio.error import S3Error
import os
import uuid
def upload_to_minio(local_image_path, bucket_name, object_name=None):
    try:
        # 初始化 MinIO 客户端
        minio_client = Minio(
            "10.21.221.12:9000",
            access_key="minioadmin",
            secret_key="Admin@hd2019",
            secure=False
        )
        # 如果没有提供object_name，生成随机文件名
        if object_name is None:
            file_extension = os.path.splitext(local_image_path)[1]
            object_name = f"{str(uuid.uuid4())}{file_extension}"

        # 确保bucket存在，不存在则创建
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        result = minio_client.fput_object(
            bucket_name, object_name, local_image_path,
        )
        print(
            f"http://10.21.221.12:9000/{bucket_name}/{object_name}"
        )
        return f"http://10.21.221.12:9000/{bucket_name}/{object_name}"
    except S3Error as err:
        print(f"上传失败: {err}")
        return False

# 使用示例
if __name__ == "__main__":
    #本地文件路径，根据各人本地要上传的文件路径修改
    local_path = 'data.csv'
    bucket_name = "timedata"
    # object_name现在是可选参数，不提供时会自动生成随机名称
    upload_to_minio(local_path, bucket_name)