from pyspark.sql.functions import col, lit, aes_encrypt, base64

class CryptoUtils:

    @staticmethod
    def encrypt_pii_data(df, columns_to_encrypt):
        encrypted_df = df
        for column in columns_to_encrypt:
            encrypted_df = encrypted_df.withColumn(
                column, 
                base64(aes_encrypt(col(column).cast("string"), lit(CryptoUtils.AES_KEY)))
            )
        return encrypted_df