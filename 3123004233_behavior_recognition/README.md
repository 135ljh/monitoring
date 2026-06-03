# 使用手册

## 注意事项

开发完之后，装个插件，运行

```bash 
pip install pipreqs -i https://pypi.tuna.tsinghua.edu.cn/simple
```

在工程路径内运行命令

```bash
pipreqs ./ --force --pypi-server http://mirrors.aliyun.com/pypi/simple
```

相比 `pip freeze>requirements.txt` 这种方式生成python所需库列表文件，该命令只生成项目所需库依赖，但是偶尔会出现重复的依赖，麻烦检查

如果用到cv2请用带_headless版本：比如 opencv_python_headless==4.7.0.72

本地手动启动方式：

```bash
python3 -u init.py '{"isdp.instance-id":"工程名-1","idsp.nacos":"nacos地址"}'
```