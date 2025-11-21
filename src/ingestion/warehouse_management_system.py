import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_inventory_data():
    """
    Generate multi-sheet Excel workbook with warehouse/inventory data
    Typical issues:
    - Multiple sheets with different schemas
    - Merged cells and formatting issues
    - Header rows not in first row
    - Formulas instead of values
    - Hidden sheets/columns
    """
    
    # Sheet 1: Warehouse Inventory
    print("Generating Sheet 1: Warehouse Inventory...")
    inventory_records = []
    
    for i in range(1, 800):
        product_id = f"PROD{random.randint(1, 500):06d}"
        warehouse_id = f"WH{random.randint(1, 10):03d}"
        
        inventory_records.append({
            'Warehouse ID': warehouse_id,
            'Product ID': product_id,
            'SKU': f"SKU-{random.randint(10000, 99999)}",
            'Current Stock': random.randint(0, 1000),
            'Reserved Stock': random.randint(0, 100),
            'Available Stock': '',  # Formula: Current - Reserved
            'Reorder Point': random.randint(10, 100),
            'Reorder Quantity': random.randint(50, 500),
            'Last Restocked': (datetime.now() - timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d'),
            'Lead Time (days)': random.randint(1, 30),
            'Unit Cost': round(random.uniform(5, 200), 2),
            'Total Value': '',  # Formula: Current Stock * Unit Cost
            'Location': f"Aisle-{random.randint(1, 20)}-Bin-{random.randint(1, 50)}",
            'Condition': random.choice(['New', 'NEW', 'new', 'Refurbished', 'Damaged', '', None])
        })
    
    inventory_df = pd.DataFrame(inventory_records)
    
    # Calculate formula fields (simulating Excel formulas)
    inventory_df['Available Stock'] = inventory_df['Current Stock'] - inventory_df['Reserved Stock']
    inventory_df['Total Value'] = inventory_df['Current Stock'] * inventory_df['Unit Cost']
    
    # Introduce some calculation errors
    mask = np.random.random(len(inventory_df)) < 0.02
    inventory_df.loc[mask, 'Available Stock'] = None
    inventory_df.loc[mask, 'Total Value'] = '#DIV/0!'  # Excel error
    
    
    # Sheet 2: Warehouse Locations
    print("Generating Sheet 2: Warehouse Locations...")
    warehouses = []
    
    warehouse_names = {
        'WH001': 'Los Angeles Distribution Center',
        'WH002': 'New York Fulfillment Center',
        'WH003': 'Chicago Hub',
        'WH004': 'Dallas Warehouse',
        'WH005': 'Seattle Distribution',
        'WH006': 'Miami Fulfillment',
        'WH007': 'Phoenix Storage',
        'WH008': 'Denver Center',
        'WH009': 'Boston Hub',
        'WH010': 'Atlanta Warehouse'
    }
    
    for wh_id, name in warehouse_names.items():
        warehouses.append({
            'Warehouse_ID': wh_id,
            'Warehouse Name': name,
            'Address': f"{random.randint(100, 9999)} Main Street",
            'City': name.split()[0],
            'State': random.choice(['CA', 'NY', 'IL', 'TX', 'WA', 'FL', 'AZ', 'CO', 'MA', 'GA']),
            'ZIP': f"{random.randint(10000, 99999)}",
            'Country': random.choice(['USA', 'US', 'United States']),
            'Manager': f"Manager{random.randint(1, 50)}",
            'Phone': f"({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}",
            'Email': f"warehouse{wh_id.lower()}@techmart.com",
            'Capacity (sq ft)': random.randint(50000, 500000),
            'Operating Hours': random.choice(['24/7', '8AM-6PM', '9AM-5PM', None]),
            'Active': random.choice([True, 'Yes', 'Y', 1, 'TRUE', None])
        })
    
    warehouses_df = pd.DataFrame(warehouses)
    
    
    # Sheet 3: Stock Movements (Transactions)
    print("Generating Sheet 3: Stock Movements...")
    movements = []
    
    for i in range(2000):
        movement_date = datetime.now() - timedelta(days=random.randint(0, 90))
        
        movements.append({
            'Transaction ID': f"TXN{str(i+100000).zfill(8)}",
            'Date': movement_date.strftime('%Y-%m-%d'),
            'Time': movement_date.strftime('%H:%M:%S'),
            'Warehouse': f"WH{random.randint(1, 10):03d}",
            'Product ID': f"PROD{random.randint(1, 500):06d}",
            'Movement Type': random.choice([
                'INBOUND', 'OUTBOUND', 'TRANSFER', 'ADJUSTMENT',
                'Inbound', 'Outbound', 'inbound', 'Return', 'Damaged', ''
            ]),
            'Quantity': random.randint(-50, 200),  # Negative for outbound
            'From Location': f"Aisle-{random.randint(1, 20)}-Bin-{random.randint(1, 50)}",
            'To Location': f"Aisle-{random.randint(1, 20)}-Bin-{random.randint(1, 50)}" if random.random() > 0.3 else None,
            'Reference': f"REF{random.randint(10000, 99999)}" if random.random() > 0.2 else None,
            'User': f"user{random.randint(1, 50)}",
            'Notes': random.choice([
                'Regular movement', 'Damaged item', 'Customer return',
                'Transfer between warehouses', '', None
            ])
        })
    
    movements_df = pd.DataFrame(movements)
    
    
    # Sheet 4: Product Dimensions (for shipping)
    print("Generating Sheet 4: Product Dimensions...")
    dimensions = []
    
    for i in range(1, 501):
        product_id = f"PROD{i:06d}"
        
        dimensions.append({
            'Product_ID': product_id,
            'Length (in)': round(random.uniform(1, 48), 2),
            'Width (in)': round(random.uniform(1, 36), 2),
            'Height (in)': round(random.uniform(1, 24), 2),
            'Weight (lbs)': round(random.uniform(0.1, 50), 2),
            'Volumetric Weight': '',  # Formula
            'Package Type': random.choice([
                'Box', 'Envelope', 'Tube', 'Pallet', 'BOX', 'box', None
            ]),
            'Fragile': random.choice(['Yes', 'No', 'Y', 'N', True, False, 1, 0, None]),
            'Stackable': random.choice(['Yes', 'No', 'Y', 'N', True, False, None]),
            'Hazmat': random.choice(['Yes', 'No', False, 'N', None])
        })
    
    dimensions_df = pd.DataFrame(dimensions)
    
    # Calculate volumetric weight (Length * Width * Height / 166)
    dimensions_df['Volumetric Weight'] = (
        dimensions_df['Length (in)'] * 
        dimensions_df['Width (in)'] * 
        dimensions_df['Height (in)']
    ) / 166
    dimensions_df['Volumetric Weight'] = dimensions_df['Volumetric Weight'].round(2)
    
    return inventory_df, warehouses_df, movements_df, dimensions_df


# Generate all sheets
inventory_df, warehouses_df, movements_df, dimensions_df = generate_inventory_data()

# Create Excel writer with multiple sheets
print("\nWriting to Excel file...")
with pd.ExcelWriter('warehouse_inventory.xlsx', engine='openpyxl') as writer:
    # Sheet 1: Add some header rows before data (common in real Excel files)
    header_df = pd.DataFrame([
        ['TechMart Warehouse Management System', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['Report Generated:', datetime.now().strftime('%Y-%m-%d %H:%M'), '', '', '', '', '', '', '', '', '', '', '', ''],
        ['Confidential - Internal Use Only', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['', '', '', '', '', '', '', '', '', '', '', '', '', '']  # Empty row
    ])
    
    # Write header rows
    header_df.to_excel(writer, sheet_name='Inventory', index=False, header=False)
    
    # Write actual data starting from row 5
    inventory_df.to_excel(writer, sheet_name='Inventory', startrow=4, index=False)
    
    # Other sheets
    warehouses_df.to_excel(writer, sheet_name='Warehouses', index=False)
    movements_df.to_excel(writer, sheet_name='Stock Movements', index=False)
    dimensions_df.to_excel(writer, sheet_name='Product Dimensions', index=False)
    
    # Add a summary sheet (often found in real Excel files)
    summary_df = pd.DataFrame([
        ['Total Products:', len(dimensions_df)],
        ['Total Warehouses:', len(warehouses_df)],
        ['Total Inventory Value:', inventory_df['Total Value'].replace('#DIV/0!', 0).sum()],
        ['Last Updated:', datetime.now().strftime('%Y-%m-%d')]
    ], columns=['Metric', 'Value'])
    
    summary_df.to_excel(writer, sheet_name='Summary', index=False)

print("✓ Generated warehouse_inventory.xlsx with 5 sheets")

print("\n" + "="*60)
print("EXCEL FILE CHARACTERISTICS:")
print("="*60)
print("✓ Multiple sheets with different schemas")
print("✓ Header rows before data (non-standard)")
print("✓ Formulas and Excel errors (#DIV/0!)")
print("✓ Mixed data types in same column")
print("✓ Inconsistent naming conventions")
print("✓ Missing values throughout")
print("✓ Boolean values in multiple formats")
print("✓ Date and time in separate columns")

print("\n" + "="*60)
print("SHEET SUMMARIES:")
print("="*60)
print(f"1. Inventory: {len(inventory_df)} rows - Main inventory with formulas")
print(f"2. Warehouses: {len(warehouses_df)} rows - Warehouse master data")
print(f"3. Stock Movements: {len(movements_df)} rows - Transaction log")
print(f"4. Product Dimensions: {len(dimensions_df)} rows - Shipping dimensions")
print(f"5. Summary: Summary statistics")

print("\n" + "="*60)
print("SAMPLE DATA FROM EACH SHEET:")
print("="*60)
print("\nInventory (first 3 rows):")
print(inventory_df.head(3))

print("\nWarehouses:")
print(warehouses_df.head(3))

print("\nStock Movements:")
print(movements_df.head(3))