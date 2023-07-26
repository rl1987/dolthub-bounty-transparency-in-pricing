#!/usr/bin/python3  

import csv

import openai
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

point_id = 1

def get_embeddings(text):
    response = openai.Embedding.create(
        input=text,
        model="text-embedding-ada-002"
    )
    embeddings = response['data'][0]['embedding']
    assert len(embeddings) == 1536
    return embeddings

def prepare_db():
    client = QdrantClient("./qdrant_data")

    for collection_name in [ "cpt", "hcpcs", "ms_drg", "apr_drg", "icd10", "rev_code" ]:
        client.recreate_collection(
            collection_name=collection_name.
            vectors_config=VectorParams(size=1536, distance=Distance.DOT),
        )
    
    return client

def index_code(code, code_type, description):
    global point_id

    if code_type == 'hcpcs_cpt':
        code_type = 'hcpcs'

    embeddings = get_embeddings(description)

    operation_info = client.upsert(
        collection_name=code_type,
        wait=True,
        points=[
            PointStruct(id=point_id, vector=embeddings, payload={"description": description, "ms_drg": ms_drg}),
        ]
    )

    print(operation_info)

    point_id += 1


def index_file(client, file_path):
    in_f = open(file_path, "r")

    csv_reader = csv.DictReader(in_f)

    for row in csv_reader:
        code = row.get('code')
        code_type = row.get('code_type')
        description = row.get('description')

        index_code(code, code_type, description)

    in_f.close()

def main():
    client = prepare_db()

    for file_path in ["cpt.csv", "hcpcs.csv", "ms_drg_icd10.csv", "apr_drg.csv", "rev_code.csv" ]:
        index_file(client, file_path)

if __name__ == "__main__":
    main()

