# Go 代码框架库

### 环境要求

Go version>=1.15

### 引用框架

`import "github.com/maclon-lee/golanglib"`

### 配置文件

拷贝模板配置文件 [conf/config.toml](conf/config.toml) 放置在自己工程目录的 conf 目录或工程根目录下，并根据实际情况替换配置项信息。

### 环境变量设置
*cmd执行以下命令*
> go env -w GO111MODULE=auto <br/>
> go env -w GOPROXY=https://goproxy.io,direct <br/>
