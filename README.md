# QQ工具箱 (YuYang.QQTools) v1.1.0

QQ互动与空间工具箱插件，基于Alife 4.2.0+ (命名空间 Alife.FunctionCaller)

## 功能与调用格式

### QQ互动
- 给指定QQ用户点赞：`<sendlike userid="QQ号" times="次数(1-10)"/>`
- 群聊戳一戳：`<pokegroupmember groupid="群号" userid="QQ号"/>`
- 撤回消息：`<deletemessage messageid="消息ID"/>`
- 获取群最近消息：`<getgroupmessagehistory groupid="群号" count="数量"/>`
- 获取最近一条自己消息ID：`<getlastmessageid/>`

### QQ空间
- 发表说说：`<publishmood content="文字"/>`
- 获取说说列表：`<getmoodlist uin="QQ号(可选，默认自己)"/>`
- 删除说说：`<deletemood tid="说说ID"/>`
- 点赞说说：`<likemood authoruin="作者QQ" tid="说说ID"/>`
- 取消点赞：`<unlikemood authoruin="作者QQ" tid="说说ID"/>`

## 注意事项
- 撤回消息需先用 getgroupmessagehistory 或 getlastmessageid 拿到自己的消息ID
- QQ空间 tid 是十六进制字符串类型
- 所有函数返回结果需要显式调用 interactor.Poke() 注入
- 若点赞/发说说作用在自己账号，请检查 OneBot 连接的QQ号是否正确

## 源码
- 本插件由幼央开发，基于Alife框架，源码见 QQToolsModule.cs
