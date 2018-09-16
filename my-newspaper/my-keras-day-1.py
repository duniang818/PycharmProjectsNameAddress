# encoding: utf-8
"""
@version: 2018/9/16_V1.0
@software: PyCharm
@file: my-keras-day-1.py
@time: 2018/9/16
@author: wym
@license: Apache Licence 
"""

import keras
# Generate dummy data
import numpy as np
from keras.layers import Dense, Dropout
from keras.models import Sequential
from keras.optimizers import SGD

x_train = np.random.random((1000, 20))
y_train = keras.utils.to_categorical(np.random.randint(10, size=(1000, 1)), num_classes=10)
x_test = np.random.random((100, 20))
y_test = keras.utils.to_categorical(np.random.randint(10, size=(100, 1)), num_classes=10)

model = Sequential()
# Dense(64) is a fully-connected layer with 64 hidden units.
# in the first layer, you must specify the expected input data shape:
# here, 20-dimensional vectors.
model.add(Dense(64, activation='relu', input_dim=20))
model.add(Dropout(0.5))
model.add(Dense(64, activation='relu'))
model.add(Dropout(0.5))
model.add(Dense(10, activation='softmax'))

sgd = SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)
model.compile(loss='categorical_crossentropy',
              optimizer=sgd,
              metrics=['accuracy'])

model.fit(x_train, y_train,
          epochs=20,
          batch_size=128)
score = model.evaluate(x_test, y_test, batch_size=128)
"""
重要概念：
"batch", "epoch"和"sample"都是啥意思？？

下面是一些使用keras时常会遇到的概念，我们来简单解释。

Sample：样本，数据集中的一条数据。例如图片数据集中的一张图片，语音数据中的一段音频。
Batch：中文为批，一个batch由若干条数据构成。batch是进行网络优化的基本单位，网络参数的每一轮优化需要使用一个batch。batch中的样本是被并行处理的。与单个样本相比，一个batch的数据能更好的模拟数据集的分布，batch越大则对输入数据分布模拟的越好，反应在网络训练上，则体现为能让网络训练的方向“更加正确”。但另一方面，一个batch也只能让网络的参数更新一次，因此网络参数的迭代会较慢。在测试网络的时候，应该在条件的允许的范围内尽量使用更大的batch，这样计算效率会更高。
Epoch，epoch可译为“轮次”。如果说每个batch对应网络的一次更新的话，一个epoch对应的就是网络的一轮更新。每一轮更新中网络更新的次数可以随意，但通常会设置为遍历一遍数据集。因此一个epoch的含义是模型完整的看了一遍数据集。 设置epoch的主要作用是把模型的训练的整个训练过程分为若干个段，这样我们可以更好的观察和调整模型的训练。Keras中，当指定了验证集时，每个epoch执行完后都会运行一次验证集以确定模型的性能。另外，我们可以使用回调函数在每个epoch的训练前后执行一些操作，如调整学习率，打印目前模型的一些信息等，详情请参考Callback一节。
深度学习框架中涉及很多参数，如果一些基本的参数如果不了解，那么你去看任何一个深度学习框架是都会觉得很困难，下面介绍几个新手常问的几个参数。

batch
深度学习的优化算法，说白了就是梯度下降。每次的参数更新有两种方式。

第一种，遍历全部数据集算一次损失函数，然后算函数对各个参数的梯度，更新梯度。这种方法每更新一次参数都要把数据集里的所有样本都看一遍，计算量开销大，计算速度慢，不支持在线学习，这称为Batch gradient descent，批梯度下降。

另一种，每看一个数据就算一下损失函数，然后求梯度更新参数，这个称为随机梯度下降，stochastic gradient descent。这个方法速度比较快，但是收敛性能不太好，可能在最优点附近晃来晃去，hit不到最优点。两次参数的更新也有可能互相抵消掉，造成目标函数震荡的比较剧烈。

为了克服两种方法的缺点，现在一般采用的是一种折中手段，mini-batch gradient decent，小批的梯度下降，这种方法把数据分为若干个批，按批来更新参数，这样，一个批中的一组数据共同决定了本次梯度的方向，下降起来就不容易跑偏，减少了随机性。另一方面因为批的样本数与整个数据集相比小了很多，计算量也不是很大。

基本上现在的梯度下降都是基于mini-batch的，所以深度学习框架的函数中经常会出现batch_size，就是指这个。 
关于如何将训练样本转换从batch_size的格式可以参考训练样本的batch_size数据的准备。

iterations
iterations（迭代）：每一次迭代都是一次权重更新，每一次权重更新需要batch_size个数据进行Forward运算得到损失函数，再BP算法更新参数。1个iteration等于使用batchsize个样本训练一次。

epochs
epochs被定义为向前和向后传播中所有批次的单次训练迭代。这意味着1个周期是整个输入数据的单次向前和向后传递。简单说，epochs指的就是训练过程中数据将被“轮”多少次，就这样。

举个例子

训练集有1000个样本，batchsize=10，那么： 
训练完整个样本集需要： 
100次iteration，1次epoch。

具体的计算公式为： 
one epoch = numbers of iterations = N = 训练样本的数量/batch_size

注：

在LSTM中我们还会遇到一个seq_length,其实 
batch_size = num_steps * seq_length
"""
"""
keras.layers.core.Dense(
units, #代表该层的输出维度
activation=None, #激活函数.但是默认 liner
use_bias=True, #是否使用b
kernel_initializer='glorot_uniform', #初始化w权重，keras/initializers.py
bias_initializer='zeros', #初始化b权重
kernel_regularizer=None, #施加在权重w上的正则项,keras/regularizer.py
bias_regularizer=None, #施加在偏置向量b上的正则项
activity_regularizer=None, #施加在输出上的正则项
kernel_constraint=None, #施加在权重w上的约束项
bias_constraint=None #施加在偏置b上的约束项
)

# 所实现的运算是output = activation(dot(input, kernel)+bias)
# model.add(Dense(units=64, activation='relu', input_dim=784))

# keras初始化所有激活函数,activation:
# keras\activations.py
# keras\backend\cntk_backend.py
# import cntk as C
# 1.softmax：
#             对输入数据的最后一维进行softmax，一般用在输出层;
#     ndim == 2,K.softmax(x),其实调用的是cntk，是一个模块;
#     ndim >= 2,e = K.exp(x - K.max(x)),s = K.sum(e),return e / s
# 2.elu
#     K.elu(x)
# 3.selu: 可伸缩的指数线性单元
#     alpha = 1.6732632423543772848170429916717
#     scale = 1.0507009873554804934193349852946
#     return scale * K.elu(x, alpha)
# 4.softplus
#     C.softplus(x)
# 5.softsign
#     return x / (1 + C.abs(x))
# 6.relu
#     def relu(x, alpha=0., max_value=None):
#         if alpha != 0.:
#             negative_part = C.relu(-x)
#         x = C.relu(x)
#         if max_value is not None:
#             x = C.clip(x, 0.0, max_value)
#         if alpha != 0.:
#             x -= alpha * negative_part
#         return x
# 7.tanh
#     return C.tanh(x)
# 8.sigmoid
#     return C.sigmoid(x)
# 9.hard_sigmoid
#     x = (0.2 * x) + 0.5
#     x = C.clip(x, 0.0, 1.0)
#     return x
# 10.linear
#     return x

# keras初始化所有方法，initializer:
# Zeros
# Ones
# Constant(固定一个值)
# RandomNormal(正态分布)
# RandomUniform(均匀分布)
# TruncatedNormal(截尾高斯分布,神经网络权重和滤波器的推荐初始化方法)
# VarianceScaling(该初始化方法能够自适应目标张量的shape)
# Orthogonal(随机正交矩阵初始化)
# Identiy(单位矩阵初始化，仅适用于2D方阵)
# lecun_uniform(LeCun均匀分布初始化)
# lecun_normal(LeCun正态分布初始化)
# glorot_normal(Glorot正态分布初始化)
# glorot_uniform(Glorot均匀分布初始化)
# he_normal(He正态分布初始化)
# he_uniform(He均匀分布初始化,Keras中文文档写错了)

# keras正则化，regularizer:
# import backend as K
# L1: regularization += K.sum(self.l1 * K.abs(x))
# L2: regularization += K.sum(self.l2 * K.square(x))
"""

"""
采用stateful LSTM的相同模型
stateful LSTM的特点是，在处理过一个batch的训练数据后，其内部状态（记忆）会被作为下一个batch的训练数据的初始状态。状态LSTM使得我们可以在合理的计算复杂度内处理较长序列

请FAQ中关于stateful LSTM的部分获取更多信息
"""
from keras.models import Sequential
from keras.layers import LSTM, Dense
import numpy as np

data_dim = 16
timesteps = 8
num_classes = 10
batch_size = 32

# Expected input batch shape: (batch_size, timesteps, data_dim)
# Note that we have to provide the full batch_input_shape since the network is stateful.
# the sample of index i in batch k is the follow-up for the sample i in batch k-1.
model = Sequential()
model.add(LSTM(32, return_sequences=True, stateful=True,
               batch_input_shape=(batch_size, timesteps, data_dim)))
model.add(LSTM(32, return_sequences=True, stateful=True))
model.add(LSTM(32, stateful=True))
model.add(Dense(10, activation='softmax'))

model.compile(loss='categorical_crossentropy',
              optimizer='rmsprop',
              metrics=['accuracy'])

# Generate dummy training data
x_train = np.random.random((batch_size * 10, timesteps, data_dim))
y_train = np.random.random((batch_size * 10, num_classes))

# Generate dummy validation data
x_val = np.random.random((batch_size * 3, timesteps, data_dim))
y_val = np.random.random((batch_size * 3, num_classes))

model.fit(x_train, y_train,
          batch_size=batch_size, epochs=5, shuffle=False,
          validation_data=(x_val, y_val))
