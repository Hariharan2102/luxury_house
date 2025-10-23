import mysql.connector

def test_data():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='harish@21',
            database='luxury_housing'
        )
        
        cursor = connection.cursor(dictionary=True)
        
        print("📊 DATA VERIFICATION:")
        print("=" * 50)
        
        # Count records
        cursor.execute("SELECT COUNT(*) as total FROM housing_sales")
        total = cursor.fetchone()['total']
        print(f"✅ Total records: {total}")
        
        # Check column mapping
        cursor.execute("SELECT * FROM housing_sales LIMIT 1")
        sample = cursor.fetchone()
        print(f"👀 Sample record keys: {list(sample.keys())}")
        
        # Check booking distribution
        cursor.execute("SELECT Booking_Flag, COUNT(*) as count FROM housing_sales GROUP BY Booking_Flag")
        print(f"📈 Booking distribution:")
        for row in cursor.fetchall():
            print(f"   Booking_Flag {row['Booking_Flag']}: {row['count']} records")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

test_data()