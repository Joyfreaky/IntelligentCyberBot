import tensorflow as tf
print(tf.__version__)
print("Num GPUs Available: ", len(tf.config.experimental.list_physical_devices('GPU')))
import time

# Ensure TensorFlow is using GPU
if tf.test.gpu_device_name():
    print('Default GPU Device: {}'.format(tf.test.gpu_device_name()))
else:
    print("Please install GPU version of TF")

# Create two large random matrices
size = 5000
a = tf.random.uniform([size, size])
b = tf.random.uniform([size, size])

# Compute dot product and measure the time taken
start = time.time()
c = tf.matmul(a, b)
end = time.time()

# Ensure the computation is executed
tf.debugging.assert_all_finite(c, "Result is not finite")

print("Time taken to compute dot product of two {}x{} matrices: {} seconds".format(size, size, end - start))