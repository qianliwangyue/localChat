# 运行方法

##### (仓库中提供了windows版本的程序压缩包[localchat.zip解压后的localchat.exe可直接使用])

## 编译方法
##### 先clone仓库
* 项目根目录下 go run .  [ 或直接运行 main.go ]能运行后再打包编译成Windows版本软件
# 打包成Windows 程序软件
## 项目根目录下
###### 安装fyne命令工具:
    go install fyne.io/fyne/v2/cmd/fyne@latest  
###### 打包

# windows打包命令: 
    fyne package -os windows -icon 1.ico -release -app-id com.ggbond.boom

###### 注意:Linux环境下C++默认已经安装，fyne环境几乎无需配置，
###### 但是Windows环境下C++需要安装，等环境，具体见fyne官网：
###### [fyne官网](https://docs.fyne.io/)
