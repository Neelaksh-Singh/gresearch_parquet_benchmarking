import re
import sys

def modify_file(filename):
    with open(filename, 'r') as file:
        content = file.read()

    # Regular expression to match VerifyField calls
    pattern = r'VerifyField<([^>]+)>\s*\(verifier,\s*([^)]+)\)'
    
    # Function to replace matches
    def replace_func(match):
        type_name = match.group(1)
        args = match.group(2)
        return f'VerifyField<{type_name}>(verifier, {args}, sizeof({type_name}))'

    # Perform the replacement
    modified_content = re.sub(pattern, replace_func, content)

    # Write the modified content back to the file
    with open(filename, 'w') as file:
        file.write(modified_content)

    print(f"Modified {filename}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_generated_header>")
        sys.exit(1)

    header_file = sys.argv[1]
    modify_file(header_file)