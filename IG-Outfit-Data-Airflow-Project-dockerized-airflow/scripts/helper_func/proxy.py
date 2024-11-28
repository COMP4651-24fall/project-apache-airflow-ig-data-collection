import os
def activate(http_proxy, https_proxy):
    os.environ["HTTP_PROXY"] = http_proxy
    os.environ["HTTPS_PROXY"] = https_proxy
    #print(f"Proxy activated. HTTP_PROXY: {http_proxy}, HTTPS_PROXY: {https_proxy}")

def deactivate():
    os.environ.pop('HTTP_PROXY', None)
    os.environ.pop('HTTPS_PROXY', None)
    #print("Proxy deactivated.")