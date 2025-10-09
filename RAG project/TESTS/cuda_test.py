#import torch
#print("CUDA available:", torch.cuda.is_available())
#print("Device count:", torch.cuda.device_count())
#print("Current device:", torch.cuda.current_device() if torch.cuda.is_available() else None)
#print("Device name:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else None)

import torch
print("Torch version:", torch.__version__)
print("Compiled with CUDA:", torch.version.cuda)
print("CUDA available:", torch.cuda.is_available())
if torch.cuda.is_available():
    print("GPU:", torch.cuda.get_device_name(0))
