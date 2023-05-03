# =============================================================================
# Generators: Yield Keyword
# =============================================================================
def fibonacci(limit):
    a, b = 0, 1
    while a < limit:
        yield a
        a, b = b, a + b

fib_gen = fibonacci(10)
for number in fib_gen:
    print(number)
    
# =============================================================================
# Using next with a generator
# =============================================================================
def even_numbers():
    num = 0
    while True:
        yield num
        num += 2

even_gen = even_numbers()

# Get the first 5 even numbers
for _ in range(5):
    print(next(even_gen))

# =============================================================================
# Using next with default value
# =============================================================================
def limited_alphabet(limit):
    for i in range(65, 65 + limit):
        yield chr(i)

alphabet_gen = limited_alphabet(5)

print(next(alphabet_gen, "End of sequence"))
print(next(alphabet_gen, "End of sequence"))
print(next(alphabet_gen, "End of sequence"))
print(next(alphabet_gen, "End of sequence"))
print(next(alphabet_gen, "End of sequence"))

# This would return "End of sequence" since there are no more elements
print(next(alphabet_gen, "End of sequence"))

# =============================================================================
# List Comprehension
# =============================================================================
numbers = [1, 2, 3, 4, 5]
squares = [x * x for x in numbers if x % 2 == 0]
print(squares)  # Output: [4, 16]

# =============================================================================
# Nested List Comprehensions
# =============================================================================
matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
flattened = [num for row in matrix for num in row]
print(flattened)  # Output: [1, 2, 3, 4, 5, 6, 7, 8, 9]

# =============================================================================
# Dictionary Comprehension
# =============================================================================
words = ['apple', 'banana', 'orange']
word_lengths = {word: len(word) for word in words}
print(word_lengths)  # Output: {'apple': 5, 'banana': 6, 'orange': 6}

# =============================================================================
# Set Comprehensions
# =============================================================================
input_list = ['LARGE WHITE HEART OF WICKER', 
              'MEDIUM CERAMIC TOP STORAGE JAR', 
              'POLKADOT JAR', 
              'RED T-SHIRT', 
              'Red T-shirt'
              ]
output_set = {word.lower() 
              for sentence in input_list 
              for word in sentence.split()
              }
print(output_set)  
# Output: {'large', 'white', 'heart', 'of', 'wicker', 'medium', 'ceramic', 
# 'top', 'storage', 'jar', 'polkadot', 'red', 't-shirt'}

# =============================================================================
# Generator Comprehensions
# =============================================================================
numbers = [1, 2, 3, 4, 5]
squares_generator = (x * x for x in numbers if x % 2 == 0)

for square in squares_generator:
    print(square)  # Output: 4, 16


"""
Higher-Order Functions
"""
# =============================================================================
# Storing a Function in a Variable
# =============================================================================
def greet(name):
    return f"Hello, {name}!"

greet_func = greet
print(greet_func("John"))  # Output: Hello, John!

# =============================================================================
# Passing a Function as a Parameter
# =============================================================================
def apply_operation(numbers, operation):
    return [operation(num) for num in numbers]

def square(x):
    return x**2

def cube(x):
    return x**3

numbers = [1, 2, 3, 4, 5]
print(apply_operation(numbers, square))  # Output: [1, 4, 9, 16, 25]
print(apply_operation(numbers, cube))    # Output: [1, 8, 27, 64, 125]
