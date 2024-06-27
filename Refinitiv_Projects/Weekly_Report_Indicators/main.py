from get_functions import generate_combined_pdf
import os

if __name__ == "__main__":
    api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
    output_pdf_file = os.path.join(os.path.join(os.path.abspath(os.path.dirname(__file__)), "output", "pdf"), 'Indicators.pdf')
    generate_combined_pdf(api_key, output_pdf_file)