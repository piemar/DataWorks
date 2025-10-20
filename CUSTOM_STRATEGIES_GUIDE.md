# 🚀 Creating Custom Migration Strategies

## Overview

The Volvo Data Framework makes it incredibly easy to create custom migration strategies. You can extend the default strategy or create completely custom ones for your specific needs.

## Quick Start - Hello World Strategy

Here's the simplest possible custom strategy:

```python
# my_custom_strategy.py
from framework.migrations.default_strategy import DefaultMigrationStrategy

class MyCustomStrategy(DefaultMigrationStrategy):
    def __init__(self, source_client, target_client, **kwargs):
        super().__init__(source_client, target_client)
        print("Hello World! My custom strategy is ready!")
```

That's it! Save this as `my_custom_strategy.py` and use it:

```bash
python flexible_migrate.py --strategy my_custom_strategy.py
```

## Available Base Classes

### 1. DefaultMigrationStrategy (Recommended for most cases)

Extend this for simple customizations:

```python
from framework.migrations.default_strategy import DefaultMigrationStrategy

class MyStrategy(DefaultMigrationStrategy):
    def __init__(self, source_client, target_client, **kwargs):
        # Custom resume field
        super().__init__(source_client, target_client, resume_field="created_at")
        
        # Custom transformation
        self.transform_document = self.my_transform
    
    def my_transform(self, doc):
        # Add custom field
        doc['migrated_by'] = 'my_strategy'
        return doc
```

### 2. BaseMigrationStrategy (For complete control)

For advanced customizations:

```python
from framework.migrations.engine import BaseMigrationStrategy, MigrationBatch

class MyAdvancedStrategy(BaseMigrationStrategy):
    def __init__(self, source_client, target_client, **kwargs):
        super().__init__(source_client, target_client)
        self.custom_config = kwargs.get('custom_config', 'default')
    
    async def read_batch(self, batch_size, resume_from=None):
        # Your custom batch reading logic
        pass
    
    async def get_resume_point(self):
        # Your custom resume logic
        pass
```

## Common Customization Patterns

### 1. Custom Resume Field

```python
class DateBasedStrategy(DefaultMigrationStrategy):
    def __init__(self, source_client, target_client, **kwargs):
        # Use created_at instead of _id for resume
        super().__init__(source_client, target_client, resume_field="created_at")
```

### 2. Document Transformation

```python
class EnrichmentStrategy(DefaultMigrationStrategy):
    def __init__(self, source_client, target_client, **kwargs):
        super().__init__(source_client, target_client, transform_document=self.enrich)
    
    def enrich(self, doc):
        # Add computed fields
        if 'amount' in doc:
            doc['amount_category'] = 'high' if doc['amount'] > 1000 else 'low'
        
        # Add metadata
        doc['_enriched_at'] = datetime.utcnow().isoformat()
        return doc
```

### 3. Data Validation and Cleaning

```python
class ValidationStrategy(DefaultMigrationStrategy):
    def __init__(self, source_client, target_client, **kwargs):
        super().__init__(source_client, target_client, transform_document=self.validate_and_clean)
    
    def validate_and_clean(self, doc):
        # Validate required fields
        if 'email' not in doc:
            doc['email'] = 'unknown@example.com'
        
        # Clean up data
        if 'phone' in doc:
            doc['phone'] = doc['phone'].replace('-', '').replace(' ', '')
        
        # Remove sensitive fields
        doc.pop('password', None)
        doc.pop('ssn', None)
        
        return doc
```

### 4. Custom Resume Logic

```python
class CustomResumeStrategy(DefaultMigrationStrategy):
    async def get_resume_point(self):
        # Custom logic to find resume point
        # Example: Find last migrated document by custom field
        last_doc = await self.target_client.collection.find_one(
            sort=[("custom_field", -1)]
        )
        
        if last_doc:
            return str(last_doc['custom_field'])
        return None
```

### 5. Batch Processing with Custom Logic

```python
class BatchProcessingStrategy(DefaultMigrationStrategy):
    async def read_batch(self, batch_size, resume_from=None):
        # Call parent method
        batch = await super().read_batch(batch_size, resume_from)
        
        # Add custom batch processing
        if batch.documents:
            logger.info(f"Processing batch with {len(batch.documents)} documents")
            
            # Example: Log statistics
            valid_count = sum(1 for doc in batch.documents if doc.get('status') == 'valid')
            logger.info(f"Valid documents in batch: {valid_count}")
        
        return batch
```

## Configuration Options

Pass configuration to your strategy:

```python
class ConfigurableStrategy(DefaultMigrationStrategy):
    def __init__(self, source_client, target_client, **kwargs):
        super().__init__(source_client, target_client)
        
        # Get configuration
        self.enable_validation = kwargs.get('validation_enabled', True)
        self.enable_enrichment = kwargs.get('enrichment_enabled', False)
        self.custom_field = kwargs.get('custom_field', 'default')
```

Use with configuration:

```bash
python flexible_migrate.py --strategy my_strategy.py --validation --enrichment
```

## Error Handling

```python
class RobustStrategy(DefaultMigrationStrategy):
    def transform_document(self, doc):
        try:
            # Your transformation logic
            doc = self.my_transformation(doc)
            return doc
        except Exception as e:
            logger.error(f"Error transforming document: {e}")
            # Return original document with error info
            doc['_transformation_error'] = str(e)
            return doc
    
    def my_transformation(self, doc):
        # Your logic here
        return doc
```

## Logging and Monitoring

```python
class MonitoredStrategy(DefaultMigrationStrategy):
    def __init__(self, source_client, target_client, **kwargs):
        super().__init__(source_client, target_client)
        self.stats = {
            'documents_processed': 0,
            'documents_transformed': 0,
            'errors': 0
        }
    
    def transform_document(self, doc):
        self.stats['documents_processed'] += 1
        
        try:
            doc = self.my_transform(doc)
            self.stats['documents_transformed'] += 1
            return doc
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Transformation error: {e}")
            return doc
    
    def get_migration_stats(self):
        stats = super().get_migration_stats()
        stats['custom_stats'] = self.stats
        return stats
```

## Testing Your Strategy

Create a test script:

```python
# test_my_strategy.py
import asyncio
from framework import ConfigManager, create_migration_engine, MigrationStrategyFactory

async def test_strategy():
    # Load config
    config_manager = ConfigManager("FRAMEWORK")
    config = config_manager.load_config()
    
    # Create engine
    engine = create_migration_engine(config)
    await engine.initialize()
    
    # Create strategy
    factory = MigrationStrategyFactory()
    strategy = factory.create_strategy(
        "my_custom_strategy.py",
        engine.source_client,
        engine.target_client
    )
    
    # Test reading a small batch
    batch = await strategy.read_batch(10)
    print(f"Read {len(batch.documents)} documents")
    
    # Test resume point
    resume_point = await strategy.get_resume_point()
    print(f"Resume point: {resume_point}")

if __name__ == "__main__":
    asyncio.run(test_strategy())
```

## Best Practices

1. **Start Simple**: Begin with `DefaultMigrationStrategy` and add customizations gradually
2. **Handle Errors**: Always wrap transformations in try-catch blocks
3. **Log Everything**: Use logging for debugging and monitoring
4. **Test Small**: Test with small batches first
5. **Document Your Strategy**: Add docstrings explaining what your strategy does
6. **Use Configuration**: Make your strategy configurable via kwargs
7. **Preserve Data**: Don't modify original data unless necessary

## Example Strategies

Check out the examples in `strategies/examples/`:

- `hello_world_strategy.py` - Simple example
- `advanced_strategy.py` - Advanced features

## Running Custom Strategies

```bash
# Use builtin strategy
python flexible_migrate.py --strategy default

# Use custom strategy
python flexible_migrate.py --strategy my_custom_strategy.py

# Use custom strategy with configuration
python flexible_migrate.py --strategy my_strategy.py --validation --enrichment

# List available strategies
python flexible_migrate.py --list-strategies
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Make sure your strategy file is in the Python path
2. **Missing Methods**: Ensure your strategy implements required methods
3. **Configuration Issues**: Check that your kwargs are properly handled
4. **Resume Point Errors**: Validate your resume field exists in documents

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

This will show detailed information about what your strategy is doing.

## Need Help?

- Check the examples in `strategies/examples/`
- Look at the builtin strategies for reference
- Use the test script to validate your strategy
- Enable debug logging to see what's happening
