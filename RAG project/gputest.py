import tensorflow as tf

gpus = tf.config.list_physical_devices('GPU')
if gpus:
    print("GPU is available!")
    for gpu in gpus:
        print(gpu)
else:
    print("No GPU available.")
