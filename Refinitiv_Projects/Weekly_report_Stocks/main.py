from Refinitiv_Projects.Weekly_report_Stocks.Scripts.Sector_tables import generate_table_html
from Refinitiv_Projects.Weekly_report_Stocks.Scripts.Sector_graphs import generate_graphs_html
import pdfkit as pdf
from PyPDF2 import PdfReader, PdfWriter
import eikon as ek
import pandas as pd


def remove_rics_for_table(sector_rics):
    rics_to_remove = ["OMVS.OM", "CIEB.CA", "UBSI.AM", "BT.TN", "AB.TN", "AMLK.DU", "AHLI.AM", "UIB.TN", "4082.SE",
                      "BH.TN", "BNA.TN",
                      "STB.TN", "UBCI.TN", "STAR.TN", "4130.SE", "ATB.TN", "SHUA.DU", "WIFAK.TN", "ASSMA.TN", "AMV.TN",
                      "CIL.TN",
                      "TRE.TN", "AMI.TN", "CLHO.CA", "UMED.TN", "POULA.TN", "OCOI.OM", "4270.SE", "CC.TN", "3008.SE",
                      "1832.SE",
                      "2180.SE", "STVR.TN", "4140.SE", "4141.SE", "AACT.OM", "TPR.TN", "TLS.TN", "ATL.TN", "TLNET.TN",
                      "SIAM.TN", "OTH.TN", "SMART.TN"]
    return [ric for ric in sector_rics if ric not in rics_to_remove]


def remove_rics_for_graph(sector_rics):
    rics_to_remove = ["OMVS.OM", "CIEB.CA", "UBSI.AM", "BT.TN", "AB.TN", "AMLK.DU", "AHLI.AM", "UIB.TN", "4082.SE",
                      "BH.TN", "BNA.TN",
                      "STB.TN", "UBCI.TN", "STAR.TN", "4130.SE", "ATB.TN", "SHUA.DU", "WIFAK.TN", "ASSMA.TN", "AMV.TN",
                      "CIL.TN",
                      "TRE.TN", "AMI.TN", "CLHO.CA", "UMED.TN", "POULA.TN", "OCOI.OM", "4270.SE", "CC.TN", "3008.SE",
                      "1832.SE",
                      "2180.SE", "STVR.TN", "4140.SE", "4141.SE", "AACT.OM", "TPR.TN", "TLS.TN", "ATL.TN", "TLNET.TN",
                      "SIAM.TN", "OTH.TN", "SMART.TN",
                      "4346.SE", "NLCS.QA", "KREK.KW", "4320.SE", '4340.SE', '4323.SE', "UPRO.DU", "NREK.KW", "ARD.CS",
                      "ADI.CS", '4347.SE', '4344.SE', '4330.SE',
                      "PHDC.CA", "MANAZEL.AD", '4348.SE', "ESHRAQ.AD", '4339.SE', "MRDS.QA", '4345.SE', '4334.SE',
                      "MAZA.KW", '4335.SE', '3007.SE',
                      '2090.SE', "4338.SE", "HELI.CA", "AAYA.KW", "NAKL.TN", "CITY.TN", "ECYCL.TN", "ARTES.TN",
                      "MNP.TN", "SMG.TN", "SOMOC.TN", "ICF.TN", "STPAP.TN"]
    return [ric for ric in sector_rics if ric not in rics_to_remove]


def convert_html_to_pdf(html_file, pdf_file, wkhtmltopdf_path):
    config = pdf.configuration(wkhtmltopdf=wkhtmltopdf_path)
    options = {
        'dpi': 300,  # Increase DPI for better quality
        'page-size': 'A4',
        'encoding': "UTF-8",
        'disable-smart-shrinking': '',
        'margin-top': '0.5in',
        'margin-right': '0.5in',
        'margin-bottom': '0.5in',
        'margin-left': '0.5in',
        'zoom': '0.25'  # Ensure the content fits within the page
    }
    pdf.from_file(html_file, pdf_file, configuration=config, options=options)


def remove_last_page(pdf_path):
    reader = PdfReader(pdf_path)
    writer = PdfWriter()

    # Add all pages except the last one
    for i in range(len(reader.pages) - 1):
        writer.add_page(reader.pages[i])

    with open(pdf_path, 'wb') as f:
        writer.write(f)


def fetch_sector_data(api_key, rics):
    ek.set_app_key(api_key)
    fields = ['TR.ICBIndustry']
    parameters = {'SDate': 0, 'EDate': 0, 'FRQ': 'D', 'Curn': 'USD'}
    data, err = ek.get_data(rics, fields, parameters)
    if err:
        raise Exception(f"Error fetching data: {err}")
    df = pd.DataFrame(data)
    return df


def generate_combined_pdf(api_key, rics, wkhtmltopdf_path, output_pdf_file):
    # Fetch sector information
    sector_df = fetch_sector_data(api_key, rics)

    # Group RICs by sector
    sectors = sector_df.groupby('ICB Industry name')['Instrument'].apply(list).to_dict()

    combined_html_file = 'htmls/combined_output.html'
    with open(combined_html_file, 'w') as outfile:
        outfile.write('<html><head><title>Combined Output</title></head><body>')
        # Generate HTML files for each sector and append them to the combined HTML
        for sector, sector_rics in sectors.items():
            filtered_table_sector_rics = remove_rics_for_table(sector_rics)
            filtered_graph_sector_rics = remove_rics_for_graph(sector_rics)
            table_html_file = generate_table_html(api_key, filtered_table_sector_rics, sector)
            plot_html_file = generate_graphs_html(api_key, filtered_graph_sector_rics, sector)
            outfile.write(f'<h1 style="font-size: 100px;">{sector}</h1>')
            # Embed the table HTML with zoom level
            with open(table_html_file) as infile:
                outfile.write('<div style="zoom: 3.8;">')  # Set zoom level here
                outfile.write(infile.read())
                outfile.write('</div>')
            # Embed the plot HTML
            with open(plot_html_file) as infile:
                outfile.write(infile.read())
            outfile.write('<div style="page-break-after: always;"></div>')
        outfile.write('</body></html>')
    # Convert the combined HTML to PDF
    convert_html_to_pdf(combined_html_file, output_pdf_file, wkhtmltopdf_path)
    # Remove the last page from the final PDF
    remove_last_page(output_pdf_file)


if __name__ == "__main__":
    api_key = 'd1591ca6f45645a7bfc517785524647492c13cbc'
    rics = ["2222.SE", "TAQA.AD", "1120.SE", "2082.SE", "2010.SE", "ADNOCGAS.AD", "1180.SE", "7010.SE", "1211.SE",
            "KFH.KW", "EAND.AD", "FAB.AD", "QNBK.QA", "DEWAA.DU", "4013.SE", "ENBD.DU", "NBKK.KW", "1060.SE", "1150.SE",
            "1010.SE", "5110.SE", "BOROUGE.AD", "IQCD.QA", "EMAR.DU", "7203.SE", "ADNOCDRILL.AD", "ADCB.AD", "2280.SE",
            "2020.SE", "QHOLDING.AD", "ALDAR.AD", "ADIB.AD", "1080.SE", "1140.SE", "1050.SE", "ADNOCDIST.AD", "QISB.QA",
            "DISB.DU", "ATW.CS", "8210.SE", "7202.SE", "7020.SE", "EMAARDEV.DU", "ORDS.QA", "IAM.CS", "4250.SE",
            "BOUK.KW",
            "ADNOCLS.AD", "1111.SE", "ADPORTS.AD", "4002.SE", "DU.DU", "AMR.AD", "4280.SE", "SALIK.DU", "2050.SE",
            "MULTIPLY.AD", "ZAIN.KW", "2223.SE", "2310.SE", "QGTS.QA", "8010.SE", "FERTIGLB.AD", "MPHC.QA", "MARK.QA",
            "BCP.CS", "NMDC.AD", "4263.SE", "ERES.QA", "2290.SE", "2382.SE", "DUBK.QA", "4030.SE", "4210.SE", "BKMB.OM",
            "4100.SE", "LHM.CS", "ALBH.BH", "COMI.CA", "4164.SE", "COMB.QA", "QEWC.QA", "1030.SE", "2250.SE", "2083.SE",
            "1020.SE", "QIIB.QA", "4004.SE", "4190.SE", "EMPOWER.DU", "BOA.CS", "BURJEEL.AD", "8230.SE", "QFLS.QA",
            "PRESIGHT.AD", "ARBK.AM", "JOPH.AM", "4142.SE", "TECOM.DU", "4300.SE", "1212.SE", "2381.SE", "2350.SE",
            "MABK.KW", "AIRA.DU", "TQM.CS", "NATB.BH", "4090.SE", "QNNC.QA", "2380.SE", "BRES.QA", "GBKK.KW", "4321.SE",
            "4200.SE", "4001.SE", "2270.SE", "DFM.DU", "4071.SE", "7030.SE", "MNG.CS", "1830.SE", "6004.SE", "4161.SE",
            "2330.SE", "4031.SE", "TMGH.CA", "EMSTEEL.AD", "TABR.DU", "NETW.L", "4015.SE", "6010.SE", "CMA.CS",
            "AGLT.KW",
            "DINV.DU", "GHITHA.AD", "BBKB.BH", "4007.SE", "4005.SE", "2060.SE", "1810.SE", "BEYON.BH", "4220.SE",
            "4009.SE", "ABKK.KW", "4020.SE", "4163.SE", "BURG.KW", "ALANSARI.DU", "4003.SE", "QAMC.QA", "MSA.CS",
            "OTEL.OM", "ADAVIATION.AD", "4162.SE", "SIB.AD", "2230.SE", "BAYANAT.AD", "VFQS.QA", "BKSB.OM", "4291.SE",
            "KPRO.KW", "IGRD.QA", "3020.SE", "MFPC.CA", "3030.SE", "CSR.CS", "ABUK.CA", "STC.KW", "1303.SE", "APEX.AD",
            "2080.SE", "9526.SE", "NIND.KW", "2081.SE", "GNAV.DU", "OQGN.OM", "4260.SE", "SWDY.CA", "YAHSAT.AD",
            "WAA.CS", "SALAM.BH", "GISS.QA", "4310.SE", "AJBNK.DU", "3050.SE", "GAZ.CS", "1322.SE", "7200.SE",
            "3040.SE", "1321.SE", "LBV.CS", "4262.SE", "7204.SE", "DANA.AD", "BOURSA.KW", "ASM.AD", "WARB.KW",
            "HUMN.KW", "TMA.CS", "ADH.CS", "2283.SE", "4322.SE", "4050.SE", "2070.SE", "BKDB.OM", "DOBK.QA", "3060.SE",
            "ETEL.CA", "AGTHIA.AD", "CIH.CS", "NBOB.OM", "BPCC.KW", "UDCD.QA", "4150.SE", "EAST.CA", "JOIB.AM",
            "SFBT.TN", "7040.SE", "GFHB.BH", "BIAT.TN", "4192.SE", "1320.SE", "ALG.KW", "TAALEEM.DU", "TGC.CS",
            "ABOB.OM", "ARMX.DU", "8030.SE", "EFIH.CA", "1202.SE", "AKT.CS", "DEYR.DU", "ATL.CS", "EKHOA.CA",
            "SREK.KW", "KIBK.KW", "2190.SE", "JAZK.KW", "CABL.KW", "WAHA.AD", "4292.SE", "1831.SE", "3010.SE",
            "2300.SE", "BCI.CS", "2040.SE", "2320.SE", "TIJK.KW", "3080.SE", "2281.SE", "4261.SE", "2170.SE",
            "MEZZ.KW", "LES.CS", "SBM.CS", "1302.SE", "8060.SE", "CAPL.AM", "3003.SE", "MERS.QA", "ESRS.CA",
            "RAKCEC.AD", "JTEL.AM", "2120.SE", "AMANT.DU", "BLDN.QA", "RAKPROP.AD", "4040.SE", "INVICTUS.AD",
            "4320.SE", "8200.SE", "IGIC.O", "MEZA.QA", "JOPT.AM", "KREK.KW", "BS.TN", "NINV.KW", "ALAF.KW",
            "OMVS.OM", "4290.SE", "4340.SE", "4323.SE", "6014.SE", "ABRJ.OM", "ORAS.CA", "ARD.CS", "BKNZ.OM",
            "NREK.KW", "AZNOULA.KW", "OCAI.OM", "4110.SE", "6002.SE", "AGHC.KW", "ADI.CS", "4012.SE", "CIEB.CA",
            "1304.SE", "GWCS.QA", "QIGD.QA", "POULA.TN", "UBSI.AM", "UPRO.DU", "ARZA.KW", "MCCS.QA", "ORDS.OM",
            "2140.SE", "SHIP.KW", "8070.SE", "2200.SE", "1833.SE", "6001.SE", "3004.SE", "8250.SE", "HRHO.CA",
            "3002.SE", "4081.SE", "FWRY.CA", "BT.TN", "1214.SE", "1183.SE", "4344.SE", "4347.SE", "4014.SE",
            "6070.SE", "4080.SE", "HPS.CS", "QFBQ.QA", "INTG.KW", "AB.TN", "2282.SE", "2150.SE", "4008.SE",
            "2340.SE", "AAYA.KW", "2370.SE", "4330.SE", "1182.SE", "8012.SE", "2030.SE", "8040.SE", "4240.SE",
            "2160.SE", "3091.SE", "2240.SE", "SID.CS", "8300.SE", "AMLK.DU", "OCOI.OM", "MCGS.QA", "8170.SE",
            "3090.SE", "3007.SE", "4180.SE", "4006.SE", "4338.SE", "2100.SE", "2001.SE", "AHLI.AM",
            "8020.SE", "1201.SE", "HELI.CA", "8120.SE", "4170.SE", "ESHRAQ.AD", "1301.SE", "JOEP.AM", "1210.SE",
            "6050.SE", "SSPW.OM", "MANAZEL.AD", "3005.SE", "UIB.TN", "CGCK.KW", "MUT.CS", "2090.SE", "4270.SE",
            "4348.SE", "CMT.CS", "PHPC.OM", "8160.SE", "RNSS.OM", "4011.SE", "1820.SE", "6090.SE", "SAH.TN",
            "1213.SE", "4082.SE", "7201.SE", "CC.TN", "OTH.TN", "SALM.QA", "PHDC.CA", "BH.TN", "6040.SE", "DH.TN",
            "OFMI.OM", "4339.SE", "2210.SE", "4051.SE", "CLHO.CA", "BNA.TN", "8270.SE", "STB.TN", "6060.SE",
            "3008.SE", "MRDS.QA", "4070.SE", "2220.SE", "4061.SE", "1832.SE", "KINS.QA", "2360.SE", "CIRA.CA",
            "4191.SE", "2180.SE", "ALIMK.KW", "4345.SE", "8310.SE", "STVR.TN", "MASR.CA", "8150.SE", "4334.SE",
            "6020.SE", "6012.SE", "4140.SE", "UBCI.TN", "8260.SE", "4141.SE", "MAZA.KW", "4335.SE", "WDAM.QA",
            "STAR.TN", "6013.SE", "2130.SE", "AACT.OM", "SALAMA.DU", "4130.SE", "NAKL.TN", "ATB.TN", "NLCS.QA",
            "4346.SE", "SHUA.DU", "ARTES.TN", "TPR.TN", "WIFAK.TN", "CITY.TN", "UMED.TN", "ASSMA.TN", "TLS.TN",
            "AMV.TN", "CIL.TN", "SMART.TN", "ICF.TN", "TRE.TN", "STPAP.TN", "ATL.TN", "MNP.TN", "ECYCL.TN",
            "SMG.TN", "TLNET.TN", "SIAM.TN", "AMI.TN", "SOMOC.TN"]
    wkhtmltopdf_path = 'C:/Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe'  # Change this path to your wkhtmltopdf path
    output_pdf_file = 'outputs/combined_output.pdf'
    generate_combined_pdf(api_key, rics, wkhtmltopdf_path, output_pdf_file)
