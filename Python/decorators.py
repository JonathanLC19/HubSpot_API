from tqdm import tqdm
from functools import wraps
import time
import concurrent.futures
import os

"""--------------------------- PROGRESS BAR ------------------------------"""
def progress_bar_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        progress_bar = kwargs.get('progress_bar', None)

        if progress_bar is None:
            # Intentar obtener el número total de iteraciones desde el generador
            total_iterations = None
            try:
                generator_arg = next(arg for arg in args if hasattr(arg, '__iter__'))
                total_iterations = len(list(generator_arg))
            except StopIteration:
                pass

            # Crear la barra de progreso
            with tqdm(total=total_iterations, desc=f'Running {func.__name__}', unit='iteration') as pbar:
                kwargs['progress_bar'] = pbar
                result = func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)

        return result

    return wrapper


"""---------------------------------- TIME DECORATOR --------------------------------------"""

def calcular_tiempo_ejecucion(func):
    def wrapper(*args, **kwargs):
        inicio = time.time()
        resultado = func(*args, **kwargs)
        fin = time.time()
        tiempo_ejecucion = fin - inicio
        print(f"Tiempo de ejecución de '{func.__name__}': {tiempo_ejecucion} segundos")
        return resultado
    return wrapper


""" ------------------------------------ MEMOIZE -------------------------------------------"""
def memoize_decorator(func):
    memo = {}
    def wrapper(*args):
        if args in memo:
            return memo[args]
        result = func(*args)
        memo[args] = result
        return result
    return wrapper



"""-------------------------------- Python decorator to parallelize any IO heavy function --------------------------------"""

def make_parallel(func):
    """
        Decorator used to decorate any function which needs to be parallized.
        After the input of the function should be a list in which each element is a instance of input fot the normal function.
        You can also pass in keyword arguements seperatley.
        :param func: function
            The instance of the function that needs to be parallelized.
        :return: function
    """

    @wraps(func)
    def wrapper(lst):
        """

        :param lst:
            The inputs of the function in a list.
        :return:
        """
        # the number of threads that can be max-spawned.
        # If the number of threads are too high, then the overhead of creating the threads will be significant.
        # Here we are choosing the number of CPUs available in the system and then multiplying it with a constant.
        # In my system, i have a total of 8 CPUs so i will be generating a maximum of 16 threads in my system.
        number_of_threads_multiple = 2 # You can change this multiple according to you requirement
        number_of_workers = int(os.cpu_count() * number_of_threads_multiple)
        if len(lst) < number_of_workers:
            # If the length of the list is low, we would only require those many number of threads.
            # Here we are avoiding creating unnecessary threads
            number_of_workers = len(lst)

        if number_of_workers:
            if number_of_workers == 1:
                # If the length of the list that needs to be parallelized is 1, there is no point in
                # parallelizing the function.
                # So we run it serially.
                result = [func(lst[0])]
            else:
                # Core Code, where we are creating max number of threads and running the decorated function in parallel.
                result = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=number_of_workers) as executer:
                    bag = {executer.submit(func, i): i for i in lst}
                    for future in concurrent.futures.as_completed(bag):
                        result.append(future.result())
        else:
            result = []
        return result
    return wrapper