"""
Data models for Volvo service orders
VERSION: 2.0 - Fixed all fake.random_number() issues
"""
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from faker import Faker
import random

fake = Faker()

class ServiceOrderItem(BaseModel):
    """Individual service item within a service order"""
    item_id: str = Field(..., description="Unique item identifier")
    service_type: str = Field(..., description="Type of service performed")
    description: str = Field(..., description="Detailed service description")
    quantity: int = Field(..., description="Quantity of service items")
    unit_price: float = Field(..., description="Price per unit")
    total_price: float = Field(..., description="Total price for this item")
    labor_hours: float = Field(..., description="Hours of labor required")
    parts_used: List[str] = Field(..., description="List of parts used")

class Customer(BaseModel):
    """Customer information"""
    customer_id: str = Field(..., description="Unique customer identifier")
    name: str = Field(..., description="Customer full name")
    email: str = Field(..., description="Customer email address")
    phone: str = Field(..., description="Customer phone number")
    address: Dict[str, str] = Field(..., description="Customer address")
    customer_type: str = Field(..., description="Type of customer (individual/corporate)")

class Vehicle(BaseModel):
    """Vehicle information"""
    vin: str = Field(..., description="Vehicle identification number")
    make: str = Field(default="Volvo", description="Vehicle make")
    model: str = Field(..., description="Vehicle model")
    year: int = Field(..., description="Vehicle year")
    mileage: int = Field(..., description="Current mileage")
    engine_type: str = Field(..., description="Engine type")
    color: str = Field(..., description="Vehicle color")
    license_plate: str = Field(..., description="License plate number")

class ServiceOrder(BaseModel):
    """Complete service order document"""
    order_id: str = Field(..., description="Unique service order identifier")
    customer: Customer = Field(..., description="Customer information")
    vehicle: Vehicle = Field(..., description="Vehicle information")
    service_items: List[ServiceOrderItem] = Field(..., description="List of service items")
    order_date: datetime = Field(..., description="Date when order was created")
    service_date: datetime = Field(..., description="Date when service was performed")
    completion_date: Optional[datetime] = Field(None, description="Date when service was completed")
    status: str = Field(..., description="Order status")
    total_amount: float = Field(..., description="Total order amount")
    labor_cost: float = Field(..., description="Total labor cost")
    parts_cost: float = Field(..., description="Total parts cost")
    tax_amount: float = Field(..., description="Tax amount")
    technician_id: str = Field(..., description="ID of assigned technician")
    service_center_id: str = Field(..., description="ID of service center")
    warranty_info: Dict[str, Any] = Field(..., description="Warranty information")
    notes: str = Field(..., description="Additional notes")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        populate_by_name = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ServiceOrderGenerator:
    """Generator for creating realistic Volvo service order data"""
    
    def __init__(self):
        self.fake = Faker()
        self.volvo_models = [
            "XC90", "XC60", "XC40", "S90", "S60", "V90", "V60", "V40",
            "C40", "EX90", "EX30", "EM90"
        ]
        self.service_types = [
            "Oil Change", "Brake Service", "Engine Diagnostic", "Transmission Service",
            "Tire Rotation", "Battery Replacement", "AC Service", "Timing Belt",
            "Spark Plug Replacement", "Air Filter", "Fuel Filter", "Coolant Flush",
            "Power Steering", "Suspension", "Exhaust System", "Electrical Diagnostic"
        ]
        self.technician_ids = [f"TECH_{i:04d}" for i in range(1, 101)]
        self.service_center_ids = [f"SC_{i:03d}" for i in range(1, 21)]
        self.parts_catalog = [
            "Oil Filter", "Air Filter", "Brake Pads", "Brake Rotors", "Spark Plugs",
            "Battery", "Timing Belt", "Water Pump", "Alternator", "Starter Motor",
            "Fuel Filter", "Transmission Fluid", "Coolant", "Power Steering Fluid",
            "Brake Fluid", "Windshield Wipers", "Headlight Bulb", "Tail Light Bulb"
        ]

    def generate_vin(self) -> str:
        """Generate a realistic VIN for Volvo vehicles"""
        # Volvo VIN format: YV1 (Volvo), then 14 more characters
        import string
        import random
        letters = ''.join(random.choices(string.ascii_uppercase, k=14))
        return f"YV1{letters}"

    def generate_license_plate(self) -> str:
        """Generate a realistic license plate"""
        import string
        import random
        letters = ''.join(random.choices(string.ascii_uppercase, k=3))
        numbers = str(random.randint(100, 999))
        return f"{letters}{numbers}"

    def generate_customer(self) -> Customer:
        """Generate a realistic customer"""
        return Customer(
            customer_id=f"CUST_{random.randint(10000000, 99999999)}",
            name=self.fake.name(),
            email=self.fake.email(),
            phone=self.fake.phone_number(),
            address={
                "street": self.fake.street_address(),
                "city": self.fake.city(),
                "state": self.fake.state(),
                "zip_code": self.fake.zipcode(),
                "country": "Sweden"
            },
            customer_type=random.choice(["individual", "corporate", "fleet"])
        )

    def generate_vehicle(self) -> Vehicle:
        """Generate a realistic Volvo vehicle"""
        year = random.randint(2015, 2024)
        return Vehicle(
            vin=self.generate_vin(),
            model=random.choice(self.volvo_models),
            year=year,
            mileage=random.randint(1000, 200000),
            engine_type=random.choice(["T5", "T6", "T8", "B5", "B6", "Electric"]),
            color=random.choice(["Black", "White", "Silver", "Blue", "Red", "Gray"]),
            license_plate=self.generate_license_plate()
        )

    def generate_service_item(self) -> ServiceOrderItem:
        """Generate a realistic service item"""
        service_type = random.choice(self.service_types)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(50, 500), 2)
        total_price = round(quantity * unit_price, 2)
        labor_hours = round(random.uniform(0.5, 8.0), 1)
        
        # Generate parts used
        num_parts = random.randint(1, 4)
        parts_used = random.sample(self.parts_catalog, min(num_parts, len(self.parts_catalog)))
        
        return ServiceOrderItem(
            item_id=f"ITEM_{random.randint(100000, 999999)}",
            service_type=service_type,
            description=f"{service_type} service for Volvo vehicle",
            quantity=quantity,
            unit_price=unit_price,
            total_price=total_price,
            labor_hours=labor_hours,
            parts_used=parts_used
        )

    def generate_service_order(self) -> ServiceOrder:
        """Generate a complete service order"""
        order_date = self.fake.date_time_between(start_date="-2y", end_date="now")
        service_date = order_date + timedelta(days=random.randint(1, 30))
        
        # 80% chance of completion
        completion_date = None
        if random.random() < 0.8:
            completion_date = service_date + timedelta(hours=random.randint(1, 48))
        
        status = "completed" if completion_date else random.choice(["pending", "in_progress", "scheduled"])
        
        # Generate service items (1-5 items per order)
        num_items = random.randint(1, 5)
        service_items = [self.generate_service_item() for _ in range(num_items)]
        
        # Calculate totals
        total_amount = sum(item.total_price for item in service_items)
        labor_cost = sum(item.labor_hours * 120 for item in service_items)  # $120/hour labor rate
        parts_cost = total_amount * 0.6  # Assume 60% parts cost
        tax_amount = round((total_amount + labor_cost) * 0.25, 2)  # 25% VAT
        
        from bson import ObjectId
        
        return ServiceOrder(
            order_id=f"SO_{random.randint(1000000000, 9999999999)}",
            customer=self.generate_customer(),
            vehicle=self.generate_vehicle(),
            service_items=service_items,
            order_date=order_date,
            service_date=service_date,
            completion_date=completion_date,
            status=status,
            total_amount=round(total_amount + labor_cost + tax_amount, 2),
            labor_cost=round(labor_cost, 2),
            parts_cost=round(parts_cost, 2),
            tax_amount=tax_amount,
            technician_id=random.choice(self.technician_ids),
            service_center_id=random.choice(self.service_center_ids),
            warranty_info={
                "covered": random.choice([True, False]),
                "warranty_type": random.choice(["manufacturer", "extended", "none"]),
                "expiry_date": self.fake.future_date(end_date="+5y").isoformat() if random.random() < 0.7 else None
            },
            notes=self.fake.text(max_nb_chars=200) if random.random() < 0.3 else ""
        )
