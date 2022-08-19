class Block:

    def __init__(self, records, next_block: str, block_size: int):
        self.records = []
        # self.block_id = curr_block
        for record in records:
            record = record.strip('()\n').split(',')
            transaction_id = int(record[0])
            transaction_sale_amount = int(record[1])
            customer_name = str(record[2]).replace("'","").replace('"', '').replace("//","").replace(" ","")
            category_of_item = int(record[3])
            self.records.append((transaction_id, transaction_sale_amount, customer_name, category_of_item))
        self.next_block = next_block
        self.block_size = block_size
        self.current_block_record_index = 0

    def add_record(self, record):
        self.records.append(record)

    def size(self) -> int:
        return len(self.records)
    
