import xml.etree.ElementTree as ET
from xml.dom import minidom
import random
from datetime import datetime, timedelta

def generate_product_catalog_xml(num_products=500):
    """
    Generates product catalog in XML format with typical issues:
    - Inconsistent XML structure
    - Special characters causing parsing issues
    - Missing required fields
    - CDATA sections
    - Nested attributes
    """
    
    # Create root element
    root = ET.Element('ProductCatalog')
    root.set('generated_at', datetime.now().isoformat())
    root.set('version', '2.1')
    root.set('xmlns', 'http://techmart.com/products/v2')
    
    categories = {
        'Electronics': ['Laptops', 'Smartphones', 'Tablets', 'Headphones', 'Cameras'],
        'Clothing': ['Shirts', 'Pants', 'Shoes', 'Accessories'],
        'Home & Garden': ['Furniture', 'Kitchen', 'Decor', 'Tools'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports']
    }
    
    brands = ['TechPro', 'SmartHome', 'FashionPlus', 'EcoLiving', 'SportMax', 
              'Premium', 'Budget', 'Luxury', '', None]
    
    for i in range(1, num_products + 1):
        product = ET.SubElement(root, 'Product')
        
        # Product ID - sometimes with different attribute names
        if random.random() > 0.05:
            product.set('id', f"PROD{i:06d}")
        else:
            product.set('product_id', f"PROD{i:06d}")  # Inconsistent naming
        
        product.set('status', random.choice(['active', 'Active', 'ACTIVE', 'inactive', 'discontinued', '']))
        
        # Basic fields
        sku = ET.SubElement(product, 'SKU')
        sku.text = f"SKU-{random.randint(10000, 99999)}"
        
        title = ET.SubElement(product, 'Title')
        # Introduce special characters and encoding issues
        special_chars = ['&', '<', '>', '"', "'", '®', '™', '©']
        title_text = f"Product {i} - Premium Quality"
        if random.random() > 0.8:
            title_text += f" {random.choice(special_chars)} Special Edition"
        title.text = title_text
        
        # Description with CDATA (sometimes)
        description = ET.SubElement(product, 'Description')
        desc_text = f"This is a detailed description for product {i}. Features include: high quality, durability, and excellent performance. <b>Special Offer!</b>"
        if random.random() > 0.5:
            # description.text should handle this, but we'll create CDATA-like content
            description.text = f"<![CDATA[{desc_text}]]>"
        else:
            description.text = desc_text
        
        # Category structure - sometimes nested, sometimes flat
        main_category = random.choice(list(categories.keys()))
        if random.random() > 0.2:
            category_elem = ET.SubElement(product, 'Category')
            main_cat = ET.SubElement(category_elem, 'MainCategory')
            main_cat.text = main_category
            sub_cat = ET.SubElement(category_elem, 'SubCategory')
            sub_cat.text = random.choice(categories[main_category])
        else:
            # Flat structure (inconsistency)
            category = ET.SubElement(product, 'Category')
            category.text = main_category
        
        # Brand - sometimes missing
        if random.random() > 0.1:
            brand = ET.SubElement(product, 'Brand')
            brand.text = random.choice(brands)
        
        # Pricing - complex nested structure
        pricing = ET.SubElement(product, 'Pricing')
        pricing.set('currency', random.choice(['USD', 'usd', '$', 'US$']))
        
        list_price = ET.SubElement(pricing, 'ListPrice')
        list_price.text = f"{random.uniform(10, 1000):.2f}"
        
        sale_price = ET.SubElement(pricing, 'SalePrice')
        base_price = float(list_price.text)
        if random.random() > 0.6:
            sale_price.text = f"{base_price * random.uniform(0.7, 0.95):.2f}"
        else:
            sale_price.text = list_price.text
        
        # Sometimes price is missing or invalid
        if random.random() < 0.02:
            list_price.text = ''
        if random.random() < 0.01:
            sale_price.text = '-10.50'  # Invalid negative price
        
        # Inventory
        inventory = ET.SubElement(product, 'Inventory')
        
        stock = ET.SubElement(inventory, 'StockQuantity')
        stock.text = str(random.randint(0, 500))
        stock.set('warehouse', f"WH{random.randint(1, 10):03d}")
        
        availability = ET.SubElement(inventory, 'Availability')
        availability.text = random.choice(['in_stock', 'In Stock', 'IN_STOCK', 
                                          'out_of_stock', 'backordered', 'discontinued', ''])
        
        # Attributes - sometimes present
        if random.random() > 0.3:
            attributes = ET.SubElement(product, 'Attributes')
            
            # Different attributes for different products
            num_attributes = random.randint(1, 5)
            for j in range(num_attributes):
                attr = ET.SubElement(attributes, 'Attribute')
                attr_names = ['Color', 'Size', 'Weight', 'Material', 'Dimensions', 
                             'color', 'size', 'COLOR']  # Inconsistent casing
                attr.set('name', random.choice(attr_names))
                attr.text = f"Value_{j}"
        
        # Images - variable structure
        if random.random() > 0.2:
            images = ET.SubElement(product, 'Images')
            num_images = random.randint(1, 4)
            for j in range(num_images):
                image = ET.SubElement(images, 'Image')
                image.set('type', random.choice(['primary', 'thumbnail', 'alternate']))
                image.text = f"https://cdn.techmart.com/images/prod{i}_{j}.jpg"
        
        # Reviews summary
        if random.random() > 0.4:
            reviews = ET.SubElement(product, 'Reviews')
            avg_rating = ET.SubElement(reviews, 'AverageRating')
            avg_rating.text = f"{random.uniform(1, 5):.1f}"
            review_count = ET.SubElement(reviews, 'ReviewCount')
            review_count.text = str(random.randint(0, 500))
        
        # Shipping info
        shipping = ET.SubElement(product, 'Shipping')
        weight = ET.SubElement(shipping, 'Weight')
        weight.text = f"{random.uniform(0.1, 50):.2f}"
        weight.set('unit', random.choice(['kg', 'KG', 'lb', 'LB', 'lbs', '']))
        
        dimensions = ET.SubElement(shipping, 'Dimensions')
        dimensions.text = f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(5, 100)}"
        dimensions.set('unit', random.choice(['cm', 'CM', 'in', 'inches', '']))
        
        # Dates
        dates = ET.SubElement(product, 'Dates')
        
        created = ET.SubElement(dates, 'Created')
        created_date = datetime.now() - timedelta(days=random.randint(1, 730))
        # Inconsistent date formats in XML
        created.text = random.choice([
            created_date.strftime('%Y-%m-%d'),
            created_date.strftime('%m/%d/%Y'),
            created_date.isoformat()
        ])
        
        updated = ET.SubElement(dates, 'LastUpdated')
        updated_date = created_date + timedelta(days=random.randint(0, 30))
        updated.text = updated_date.isoformat()
    
    return root


# Generate XML
print("Generating product catalog XML...")
catalog = generate_product_catalog_xml(500)

# Convert to string with pretty printing
xml_string = ET.tostring(catalog, encoding='unicode')
dom = minidom.parseString(xml_string)
pretty_xml = dom.toprettyxml(indent='  ')

# Save to file
with open('product_catalog.xml', 'w', encoding='utf-8') as f:
    f.write(pretty_xml)

print("✓ Generated product_catalog.xml with 500 products")

print("\n" + "="*60)
print("XML DATA QUALITY ISSUES:")
print("="*60)
print("✓ Inconsistent XML structure (nested vs flat)")
print("✓ Attribute name variations (id vs product_id)")
print("✓ Special characters and encoding issues")
print("✓ Missing required fields")
print("✓ CDATA sections mixed with regular text")
print("✓ Inconsistent date/number formats")
print("✓ Empty string vs missing elements")
print("✓ Invalid data (negative prices)")
print("✓ Case sensitivity issues (Active vs active)")
print("\n" + "="*60)
print("SAMPLE XML STRUCTURE:")
print("="*60)
print(pretty_xml[:2000] + "...")  # Print first 2000 chars