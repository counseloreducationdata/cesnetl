# Module with functions to extract text from source code
# Emilio Lehoucq

# Importing libraries
from bs4 import BeautifulSoup
import re

# Defining functions
def remove_excess_line_breaks(input_string):
    '''
    Function to remove excess line breaks.
    Arg: input_string (str)
    Return: output_string (str)
    Dependencies: re
    '''
    return re.sub(r'(( \n){2,}|\n{2,})', '\n', input_string)

def remove_extra_spaces(string):
    '''
    Function to remove extra spaces.
    Arg: string (str)
    Return: string (str)
    Dependencies: re
    '''
    return re.sub(r' {2,}', ' ', string)

def remove_extra_tabs(string):
    '''
    Function to remove extra tabs.
    Arg: string (str)
    Return: string (str)
    Dependencies: re
    '''
    return re.sub(r'(( \t){2,}|\t{2,})', '\t', string)

def extract_text(html_content):
    '''
    Function to extract text from source code.
    Input: source code (str)
    Output: text (str)
    Dependencies: BeautifulSoup from bs4 and remove_excess_line_breaks, remove_extra_spaces, and remove_extra_tabs (which depend on re)
    '''
    soup = BeautifulSoup(str(html_content), 'html.parser') # Using str to avoid TypeError: object of type 'float' has no len()
    # " " to join the bits of text together
    # Not using strip=True because it removes all leading and trailing whitespaces. I want to keep some for structure
    # https://www.crummy.com/software/BeautifulSoup/bs4/doc/
    # https://www.educative.io/answers/how-to-use-gettext-in-beautiful-soup
    text = soup.get_text(" ")
    text = text.replace('\xa0', ' ') # Replace non-breaking space with regular space
    text = text.strip() # Remove leading and trailing whitespaces
    text = remove_excess_line_breaks(text)
    text = remove_extra_spaces(text)
    text = remove_extra_tabs(text)
    return text

if __name__ == "__main__":
    print("Running script as main...")
    assert extract_text("Harvard University \n \n \n \n \n \n \n O p p o r t u") == "Harvard University\n O p p o r t u"  
    assert extract_text("test      test") == "test test"
    assert extract_text("test \t \t \t test") == "test\t test"
    print("All tests passed!")