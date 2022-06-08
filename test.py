import numpy as np
import matplotlib.pyplot as plt

def sigmoid(x):
    return 1.0/(1.0 + np.exp(-x))
x = np.arange(-10,10,0.2)
y = [sigmoid(i) for i in x]
plt.grid(True)
plt.plot(x,y)
plt.show()