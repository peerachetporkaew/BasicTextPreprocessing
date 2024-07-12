# Apply Syllcut

from syllacut import tokenize
from multiprocessing import Pool
import sys
import orjson as json

class SyllacutPreprocessor(object):

    def __init__(self, n_process = 16):
        self.n_process = n_process

    def process_jsonl_file(self,fin_name, fout_name, key="text"):
        fp = open(fin_name,"r").readlines()
        fo = open(fout_name,"w")

        batch = []

        for id, line in enumerate(fp):
        
            print(id)
            text = json.loads(line.strip())[key].replace("\n","$#$")

            if id % self.n_process == 0:
                batch.append(text)
                with Pool(self.n_process) as p:
                    out = p.map(tokenize, batch )
                    fo.writelines("\n".join(out) + "\n")

                batch = []

            else:
                batch.append(text)
            

        if len(batch) > 0:
        
            with Pool(self.n_process) as p:
                out = p.map(tokenize, batch)
                fo.writelines("\n".join(out) + "\n")

        fo.close()

    def process_jsonl(self,fin_name, fout_name,key = "text"):

        fp = open(fin_name,"r").readlines()
        fo = open(fout_name,"w")

        for id, line in enumerate(fp):

            print(id)

            text = json.loads(line.strip())[key].replace("\n","$#$")
            out = tokenize(text)

            fo.writelines(out + "\n")

        fo.close()


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage : python apply_syllacut input_file output_fiile [key]")
        exit()

    if len(sys.argv) == 4:
        key = sys.argv[3]
    else:
        key = "text"
        
    processor = SyllacutPreprocessor(128)
    processor.process_jsonl(sys.argv[1], sys.argv[2],key=key)

    print("FINISH")
    
