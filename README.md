![Datax-logo](https://github.com/alibaba/DataX/blob/master/images/DataX-logo.jpg)

# 项目简介

DataX是一款十分优秀的数据同步工具，以插件的方式集成了大量数据源的读写。

但大部分情况下，我们不需要如此多种数据源的支持，例如，我目前的工作仅需要oracle、postgresql两种数据源的互转。
也就是说，数据源的类型总是确定的，不需要以插件的形式动态加入，太多的插件反而显得有些重了。

除了插件动态引入数据源的问题，datax还需要assembly打包支持，这给调试及二次开发带来了一定难度。

另一方面，datax通过py脚本来运行的方式，也不太好和其它纯java任务直接配合。

所以，本项目(datax-simple)对datax进行了精简，去掉了assembly,并将插件直接放在pom的dependencies下，得到了一个固定数据源的datax模板。

本项目支持oracle、postgresql的读写，您可根据自己的需要，参考说明文档对支持的数据源类型进行替换、增减。

# 适用场景

如果你有如下需求，这个项目应该适合你：

1、需要扩展、改造datax，或者单纯的想跑一下源码熟悉效果，但是又对assembly不熟悉(比如我o(╯□╰)o），那这个项目可以帮助你以无插件的maven工程来编写、调试代码；

2、数据源种类确定，需要一个只包含指定数据源的精简包，那你可以在这个项目上随意增减数据源插件并直接调试；

3、希望以一个纯java任务中搞定同步的全部逻辑，比如同步完后用javamail发一封邮件通知，那你可以方便地在Engine类里添加发邮件等代码。



# 使用步骤

## 0、下载本项目源码

o(*￣︶￣*)o

## 1、更换需要的数据源
本项目支持oracle、postgresql的读写，同时为了方便测试，也保留了steam数据源。
所以如果您的同步需求刚好也是oracle和postgresql，您可跳过此步骤。

从[DataX](https://github.com/alibaba/DataX)库下载源码，选择需要的module并加入datax-simple。

#### 示例
例如，需要从mysql中抽取数据。

1）复制mysqlreader工程到项目中，复制完后，您的datax-simple结构如下:

```
datax
    - common
    - core
    - mysqlreader
    - oraclereader
    - .....
    - pom.xml
```

2）在根目录的pom文件中添加mysqlreader

```
    <modules>
        <module>common</module>
        <module>core</module>
        <module>transformer</module>

        <!-- reader -->
        <module>mysqlreader</module>
        <module>postgresqlreader</module>
        <module>streamreader</module>
        <module>oraclereader</module>
        <!-- writer -->
        <module>streamwriter</module>
        <module>postgresqlwriter</module>
        <module>oraclewriter</module>
        <!-- common support module -->
        <module>plugin-rdbms-util</module>
        <module>plugin-unstructured-storage-util</module>
    </modules>

```

3)在core模块的pom文件中加入mysqlreader的依赖
```
 <dependencies>
        ....
        <!-- 需要用到的读写插件、可自行增删 -->
        <dependency>
            <artifactId>mysqlreader</artifactId>
            <groupId>com.alibaba.datax</groupId>
            <version>${datax-project-version}</version>
            <exclusions>
                <exclusion>
                    <artifactId>slf4j-log4j12</artifactId>
                    <groupId>org.slf4j</groupId>
                </exclusion>
            </exclusions>
        </dependency>
        ....

    </dependencies>
```
4)打开目录 core/src/main/resources/datax/plugin，添加对应的插件配置信息到reader/writer文件夹

本示例我们新建文件夹``core/src/main/resources/datax/plugin/reader/mysqlreader``,
将``\DataX源码\mysqlreader\src\main\resources``
下的两个json到此文件夹，此时，文件结构为
```
core/src/main/resources/datax/plugin
    - reader
        - mysqlreader
            - pluhin.jdon
            - plugin_job_template.json
        - oraclereader
        ...
    - writer
        - ...
```

## 2、运行
先按照datax的编写规范，编写几个job json，然后把json的路径分行写入
``\core\src\main\resources\datax\tasks.md``

运行com.alibaba.datax.core.Engine类即可~
o(*￣︶￣*)o
