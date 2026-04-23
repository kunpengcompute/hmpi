# Hyper MPI

## 🔥Release Notes

- [2026/03] UCG模块新增集合通信MPI_Scatter、MPI_Iscatter、MPI_Gather、MPI_Igather、MPI_Gatherv、MPI_Igatherv算法适配大规模应用场景。
- [2025/12] 修复了部分已知问题。
- [2025/07] 修复了部分已知问题。
- [2025/06] 新增Debug版本，配合定位日志增强，提升大规模作业问题定位分析效率。
- [2025/03] 支持RoH网络， 异步通信卸载。
- [2024/12] 优化了小包通信性能，新增支持RC模式多网口容错，可靠性进一步提升。
- [2024/06] 基于Open MPI 4.16、Open UCX 1.15版本使能鲲鹏硬件特性。

## 🚀概述

MPI（Message Passing Interface）是一种支持多语言编程的并行计算通信应用接口规范，他定义了一组函数和语法规则，使得多个进程可以在不同的计算机节点上协同工作，并共同完成一个任务，MPI有多种实现，例如Open MPI, Intel MPI, MPICH等。
Hyper MPI（简称HMPI）是在Open MPI的基础上，结合Open UCX (Unified Communication X)框架的点对点通信操作，开发出专为集合通信优化的UCG（Unified Communication Group）框架，该框架中实现了多种集合操作加速算法。Hyper MPI具有高性能、大规模性、可以移植等特点；支持制造，气象和基因测序等场景解决方案，致力于构建以鲲鹏服务器为坚实硬件基础的高性能计算生态。

## 📝版本配套

- 运行平台
  - 鲲鹏 920 系列
- 系统规格
  - openEuler 20.03 (LTS-SP3) AArch64 
  - openEuler 22.03 (LTS-SP2) AArch64
  - openEuler 22.03 (LTS-SP3) AArch64
  - openEuler 22.03 (LTS-SP4) AArch64
  - openEuler 24.03 (LTS-SP3) AArch64
  - Kylin Linux Advanced Server V10 (Hydrogen) AArch64
  - Kylin Linux Advanced Server V10 (Sword) AArch64
  - Kylin Linux Advanced Server Industry V10 AArch64
  - Kylin Linux Advanced Server V10 (Jasmine)
  - Kylin Linux Advanced Server V10 (GFB)
  - Kylin Linux Advanced Server V11 (Swan25)
  - KylinSec OS Linux 3 (Qomolangma) AArch64 (3.5.2)
  - KylinSec OS Linux 3 (Qomolangma) AArch64 (3.5.3)

## ⚡️编译安装

若您希望**从零到以快速体验**项目能力，参照下述编译安装教程

### 1. [获取HCPKit软件包](https://www.hikunpeng.com/document/detail/zh/kunpenghpcs/instg/KunpengHPCKit_install_007.html)

[https://www.hikunpeng.com/developer/hpc/hpckit-download](https://www.hikunpeng.com/developer/hpc/hpckit-download)

### 2. [安装HPCKit](https://www.hikunpeng.com/document/detail/zh/kunpenghpcs/instg/KunpengHPCKit_install_012.html)

#### 解压 HPCKit 软件安装包（HPCKit版本号根据实际情况调整）

~~~
tar xvf HPCKit_26.0.RC1_Linux-aarch64.tar.gz
~~~

#### 安装 HPCKit

~~~
sh HPCKit_26.0.RC1_Linux-aarch64/install.sh -y --prefix=[HPCkit安装目录]
~~~

### 3. [设置环境变量](https://www.hikunpeng.com/document/detail/zh/kunpenghpcs/instg/KunpengHPCKit_install_012.html)

#### 加载 module

~~~
module use [HPCKit安装目录]/HPCKit/latest/modulefiles
~~~

#### 加载编译器环境变量
确认您需要的编译器类型（GCC 或 Bisheng），在终端执行相应加载命令：
- 若使用 GCC （编译器版本号根据实际情况调整）：
```
module load gcc/compiler12.3.1/gccmodule
```

- 若使用 Bisheng （编译器版本号根据实际情况调整）：
```
mdoule load bisheng/compiler5.1.0.2/bishengmodule
```

### 4. 安装编译所需依赖

安装cmake，flex

```
yum install cmake
yum install flex
```

### 5. 编译流程

#### 5.1 设置编译环境目录
```
WORKSPACE=$PWD
mkdir -p ${WORKSPACE}/install
```

#### 5.2 使用git克隆项目
```
git clone  https://atomgit.com/kunpengcompute/hucx ${WORKSPACE}/hucx
git clone  https://atomgit.com/kunpengcompute/xucg ${WORKSPACE}/xucg
git clone  https://atomgit.com/kunpengcompute/hmpi ${WORKSPACE}/hmpi
```

#### 5.3 编译hucx
```
cd ${WORKSPACE}/hucx
./autogen.sh
#gcc版本编译
./contrib/configure-opt --prefix=${WORKSPACE}/install/hucx --enable-mt --disable-numa --with-pic --enable-inc --without-java CC=gcc CXX=g++ FC=gfortran
#毕昇版本编译
./contrib/configure-opt --prefix=${WORKSPACE}/install/hucx --enable-mt --disable-numa --with-pic --enable-inc --without-java CC=clang CXX=clang++ FC=flang
#配置参数：
#--enable-mt：支持多线程
#--disable-numa：禁用numctl强关联
#--without-java：HPCkit业务中没有java开发，剔除java模块，减轻UCX部署资源
make -j32 && make -j32 install
```

**添加hucx到环境变量**
```
HUCX_DIR=${WORKSPACE}/install/hucx
export LD_LIBRARY_PATH=${HUCX_DIR}/lib:$LD_LIBRARY_PATH
export PATH=${HUCX_DIR}/bin:$PATH
export C_INCLUDE_PATH=${HUCX_DIR}/include:$C_INCLUDE_PATH
export CXX_INCLUDE_PATH=${HUCX_DIR}/include:$CXX_INCLUDE_PATH
```

#### 5.4 编译xucg
```
cd ${WORKSPACE}/xucg
mkdir -p ${WORKSPACE}/xucg/build && cd ${WORKSPACE}/xucg/build
#gcc版本编译
cmake .. -DCMAKE_INSTALL_PREFIX=${WORKSPACE}/install/xucg -DCMAKE_BUILD_TYPE=Release -DUCG_BUILD_TESTS=OFF -DUCG_ENABLE_MT=ON -DUCG_BUILD_WITH_UCX=${HUCX_DIR} -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DCMAKE_Fortran_COMPILER=gfortran
#毕昇版本编译
cmake .. -DCMAKE_INSTALL_PREFIX=${WORKSPACE}/install/xucg -DCMAKE_BUILD_TYPE=Release -DUCG_BUILD_TESTS=OFF -DUCG_ENABLE_MT=ON -DUCG_BUILD_WITH_UCX=${HUCX_DIR} -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_Fortran_COMPILER=flang
make -j32 && make -j32 install
```

**添加xucg到环境变量**
```
XUCG_DIR=${WORKSPACE}/install/xucg
export LD_LIBRARY_PATH=${XUCG_DIR}/lib:$LD_LIBRARY_PATH
export PATH=${XUCG_DIR}/bin:$PATH
export C_INCLUDE_PATH=${XUCG_DIR}/include:$C_INCLUDE_PATH
export CXX_INCLUDE_PATH=${XUCG_DIR}/include:$CXX_INCLUDE_PATH
```

#### 5.5 编译hmpi
```
cd ${WORKSPACE}/hmpi
./autogen.pl
#gcc版本编译
./configure --prefix=${WORKSPACE}/install/hmpi --with-platform=contrib/platform/mellanox/optimized --enable-mpi1-compatibility --with-ucx=${HUCX_DIR} --with-ucg=${XUCG_DIR} --with-pic CC=gcc CXX=g++ FC=gfortran
#毕昇版本编译
./configure --prefix=${WORKSPACE}/install/hmpi --with-platform=contrib/platform/mellanox/optimized --enable-mpi1-compatibility --with-ucx=${HUCX_DIR} --with-ucg=${XUCG_DIR} --with-pic CC=clang CXX=clang++ FC=flang
#配置参数：
#--enable-mpi1-compatibility：启用MPI-1标准兼容性支持
make -j32 && make -j32 install
```

**添加所有的环境变量到环境中，可单独写入加载文件**
```
HUCX_DIR=${WORKSPACE}/install/hucx
XUCG_DIR=${WORKSPACE}/install/xucg
HMPI_DIR=${WORKSPACE}/install/hmpi
export OPAL_PREFIX=${HMPI_DIR}
export LD_LIBRARY_PATH=${HUCX_DIR}/lib:${XUCG_DIR}/lib:${HMPI_DIR}/lib:$LD_LIBRARY_PATH
export PATH=${HUCX_DIR}/bin:${XUCG_DIR}/bin:${HMPI_DIR}/bin:$PATH
```

### 6. 测试方法

OSU Micro Benchmark工具是开源提供的通信效率测评工具，可直接下载[osu-benchmarks-7.5.2](https://mvapich.cse.ohio-state.edu/download/mvapich/osu-micro-benchmarks-7.5.2.tar.gz)或访问[OSU测试官方网址](https://mvapich.cse.ohio-state.edu/benchmarks/)获取历史版本

#### 6.1 编译方法

```
#将下载好的osu-micro-benchmarks-7.5.2.tar.gz上传到服务器的/path/to目录下
cd /path/to
tar -xzvf osu-micro-benchmarks-7.5.2.tar.gz
cd osu-micro-benchmarks-7.5.2
./configure CC=mpicc CXX=mpicxx --prefix=/path/to/osu/install
make -j32 && make install
```

#### 6.2 测试用例

```
mpirun -N 4 --hostfile hf4 /path/to/osu/install/libexec/osu-micro-benchmarks/mpi/startup/osu_init
```

#### 6.3 常见用例

```
#启动测试
osu_init
osu_hello
#单边通信：
osu_get_bw
osu_put_bw
#点对点通信：
osu_bw
osu_latency
#集合通信:
osu_allgather
osu_alltoall
```

## 📖学习教程

若您已学习**编译安装**，对本项目有一定认知，并希望**深入了解和体验项目**，请访问下述详细教程。

1. [开发指南](https://www.hikunpeng.com/document/detail/zh/kunpenghpcs/hpckit/devg/userg_huaweimpi_0003.html)：提供详细接口开发指南，从零学习接口功能与开发。

## 🤝联系我们
本项目功能文档正在持续更新和完善中，建议您关注最新版本。
- **问题反馈**：通过[【Issues】](https://gitcode.com/kunpengcompute/hmpi/issues)提交问题。
- **社区互动**：通过[【鲲鹏社区（HPC专区）】](https://www.hikunpeng.com/forum/forum-0187135482144798003-1.html)参与交流。
- **技术专栏**：通过[【鲲鹏社区】](https://www.hikunpeng.com/developer/techArticles)获取技术文章，如系列化教程，优秀实践等。