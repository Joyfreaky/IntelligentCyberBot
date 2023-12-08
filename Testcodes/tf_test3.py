import tensorflow as tf
from tensorflow.python.framework.ops import disable_eager_execution
import time

disable_eager_execution()

print(tf.__version__)
print("Num GPUs Available: ", len(tf.config.experimental.list_physical_devices('GPU')))

# Ensure TensorFlow is using GPU
if tf.test.gpu_device_name():
    print('Default GPU Device: {}'.format(tf.test.gpu_device_name()))
else:
    print("Please install GPU version of TF")

# Create two large random matrices
size = 5000
a = tf.random.uniform([size, size])
b = tf.random.uniform([size, size])

# Start the profiler
tf.profiler.experimental.start('logdir')

# Compute dot product and measure the time taken
start = time.time()
c = tf.matmul(a, b)
end = time.time()

# Stop the profiler
tf.profiler.experimental.stop()

print("Time taken to compute dot product of two {}x{} matrices: {} seconds".format(size, size, end - start))