import torch
import torch.nn as nn
import torch.optim as optim
import time

# Define a simple linear model
model = nn.Linear(10, 2)

# Check if a GPU is available and if not, use a CPU
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f'Training on device: {device}')

# Move the model to the device
model.to(device)

# Define a loss function and an optimizer
criterion = nn.MSELoss()
optimizer = optim.SGD(model.parameters(), lr=0.01)

# Generate some dummy data
inputs = torch.randn(100, 10).to(device)
targets = torch.randn(100, 2).to(device)

start_time = time.time()

# Train the model
for i in range(100):
    # Zero the gradients
    optimizer.zero_grad()

    # Forward pass
    outputs = model(inputs)
    loss = criterion(outputs, targets)

    # Backward pass and optimization
    loss.backward()
    optimizer.step()

end_time = time.time()
print(f'Training time: {end_time - start_time} seconds')