from bs4 import BeautifulSoup

def extract_log_content(html_doc):
    """
    Extracts and returns the text within the <pre> tag that immediately follows
    an <h3> tag with the text 'Log'. Returns None if not found.

    Parameters:
        html_doc (str): A string containing HTML content.

    Returns:
        str or None: The extracted log content or None if the tag sequence isn't found.
    """
    soup = BeautifulSoup(html_doc, 'html.parser')
    h3_log = soup.find('h3', string='Log')
    if h3_log:
        pre_tag = h3_log.find_next_sibling('pre')
        if pre_tag:
            return pre_tag.get_text(strip=True)
    return None

