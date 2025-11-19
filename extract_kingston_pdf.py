"""
 Copyright 2024 Adobe
 All Rights Reserved.
 NOTICE: Adobe permits you to use, modify, and distribute this file in
 accordance with the terms of the Adobe license agreement accompanying it.
"""

import logging
import os
from datetime import datetime
import zipfile

from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.exception.exceptions import ServiceApiException, ServiceUsageException, SdkException
from adobe.pdfservices.operation.io.cloud_asset import CloudAsset
from adobe.pdfservices.operation.io.stream_asset import StreamAsset
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType

# 🔁 THIS IS THE EXTRACT API (not ExportPDF)
from adobe.pdfservices.operation.pdfjobs.jobs.extract_pdf_job import ExtractPDFJob
from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_pdf_params import ExtractPDFParams
from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_element_type import ExtractElementType
from adobe.pdfservices.operation.pdfjobs.result.extract_pdf_result import ExtractPDFResult

logging.basicConfig(level=logging.INFO)


class ExportPDFToJSON:
    def __init__(self):
        try:
            # Open your local PDF
            with open(r"C:\Users\yafet\Downloads\Zoaningkingston(test1).pdf", "rb") as file:
                input_stream = file.read()

            # Local-only credentials (DO NOT commit these to git)
            credentials = ServicePrincipalCredentials(
                client_id="",
                client_secret=""
            )

            # Create PDF Services instance
            pdf_services = PDFServices(credentials=credentials)

            # Upload source PDF
            input_asset = pdf_services.upload(
                input_stream=input_stream,
                mime_type=PDFServicesMediaType.PDF
            )

            # What to extract: text + tables (you can change this)
            extract_pdf_params = ExtractPDFParams(
                elements_to_extract=[ExtractElementType.TEXT, ExtractElementType.TABLES]
            )

            # Create Extract job
            extract_pdf_job = ExtractPDFJob(
                input_asset=input_asset,
                extract_pdf_params=extract_pdf_params
            )

            # Submit job and get result
            location = pdf_services.submit(extract_pdf_job)
            pdf_services_response = pdf_services.get_job_result(location, ExtractPDFResult)

            # Get resulting asset (this is usually a ZIP with structuredData.json inside)
            result_asset: CloudAsset = pdf_services_response.get_result().get_resource()
            stream_asset: StreamAsset = pdf_services.get_content(result_asset)

            # 1️⃣ Save raw Adobe output (ZIP)
            zip_output_path = self.create_output_zip_path()
            with open(zip_output_path, "wb") as f:
                f.write(stream_asset.get_input_stream())

            logging.info(f"Extract output ZIP saved to: {zip_output_path}")

            # 2️⃣ Extract structuredData.json from the ZIP
            json_output_path = self.create_output_json_path()
            self.extract_structured_json(zip_output_path, json_output_path)

            logging.info(f"structuredData.json extracted to: {json_output_path}")

        except (ServiceApiException, ServiceUsageException, SdkException) as e:
            logging.exception(f"Adobe SDK exception encountered: {e}")
        except Exception as e:
            logging.exception(f"Unexpected error: {e}")

    @staticmethod
    def create_output_zip_path() -> str:
        now = datetime.now()
        time_stamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        os.makedirs("output/ExtractPDF", exist_ok=True)
        return f"output/ExtractPDF/extract_{time_stamp}.zip"

    @staticmethod
    def create_output_json_path() -> str:
        now = datetime.now()
        time_stamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        os.makedirs("output/ExtractPDF/json", exist_ok=True)
        return f"output/ExtractPDF/json/structuredData_{time_stamp}.json"

    @staticmethod
    def extract_structured_json(zip_path: str, json_output_path: str) -> None:
        """Find structuredData.json inside the Adobe ZIP and save it as a standalone JSON file."""
        with zipfile.ZipFile(zip_path, "r") as zf:
            json_member = None
            for name in zf.namelist():
                # Adobe usually puts it at the root as structuredData.json
                if name.endswith("structuredData.json"):
                    json_member = name
                    break

            if json_member is None:
                raise RuntimeError("structuredData.json not found in Extract output ZIP.")

            with zf.open(json_member) as jf, open(json_output_path, "wb") as out_f:
                out_f.write(jf.read())


if __name__ == "__main__":
    ExportPDFToJSON()
