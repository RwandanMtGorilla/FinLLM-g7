import gradio as gr
from PIL import Image

# 示例函数，用于加载并返回图片
def show_image():
    # 这里可以使用本地图片的路径，或者从其他来源获取
    image = Image.open("image/多公司_货币资金_变化.png")
    return image

# 创建 Gradio 接口，使用图像输出
iface = gr.Interface(
    fn=show_image,    # 指定显示图片的函数
    inputs=[],        # 无需输入参数
    outputs="image"   # 指定输出为图像
)

# 启动接口，并将分享选项设置为True，生成访问URL
iface.launch(share=True)
