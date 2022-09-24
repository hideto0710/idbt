from ipykernel.kernelapp import IPKernelApp
from idbt.kernel import DBTKernel

IPKernelApp.launch_instance(kernel_class=DBTKernel)
