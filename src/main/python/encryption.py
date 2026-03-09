"""
Module for encrypting Personally Identifiable Information data
using AES encryption within Apache Spark DataFrames.

The AES key is loaded from the environment variable AES_KEY.
"""

import os
from pyspark.sql.functions import col, lit, aes_encrypt, base64


class CryptoUtils:
    """Utility class for AES encryption of PII data in Spark DataFrames.

    Attributes:
        aes_key (str | None): AES encryption key loaded from the
            AES_KEY environment variable. Must be 16, 24, or 32 characters.

    Example:
        >>> encrypted_df = CryptoUtils.encrypt_pii_data(df, ["Name", "Address"])
    """

    aes_key = os.environ.get("AES_KEY")

    @staticmethod
    def encrypt_pii_data(df, columns_to_encrypt):
        """Encrypts specified columns in a Spark DataFrame using AES + Base64.

        Casts each target column to string, applies AES encryption,
        and encodes the result as Base64 so it remains readable as text.

        Args:
            df (pyspark.sql.DataFrame): Input Spark DataFrame containing PII data.
            columns_to_encrypt (list[str]): List of column names to encrypt.

        Returns:
            pyspark.sql.DataFrame: DataFrame with specified columns encrypted.

        Example:
            >>> encrypted_df = CryptoUtils.encrypt_pii_data(df, ["Name", "Address"])
        """
        encrypted_df = df
        for column in columns_to_encrypt:
            encrypted_df = encrypted_df.withColumn(
                column,
                base64(aes_encrypt(col(column).cast("string"), lit(CryptoUtils.aes_key)))
            )
        return encrypted_df