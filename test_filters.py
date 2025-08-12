#!/usr/bin/env python3
"""
Test script to verify that dictionary filter format has been removed
and only tuple format is supported.
"""
import sys
sys.path.append('.')

from feature_store_sdk.projection import FeatureSourceProjection, feature_source_projection
from feature_store_sdk import FeatureStore, BatchFeatureGroup


def test_tuple_filters_work():
    """Test that tuple filters still work"""
    print("Testing tuple filter format...")
    
    # Mock feature group for testing
    class MockFeatureGroup:
        def __init__(self, name):
            self.name = name
            self.keys = ["id"]
            self.location = f"/mock/{name}"
        
        def exists(self):
            return True
    
    mock_fg = MockFeatureGroup("test")
    
    # Test single tuple filter
    try:
        proj1 = feature_source_projection(
            feature_group=mock_fg,
            features=["id", "status"],
            where=("status", "==", "ACTIVE")
        )
        print("✅ Single tuple filter works")
    except Exception as e:
        print(f"❌ Single tuple filter failed: {e}")
        return False
    
    # Test multiple tuple filters
    try:
        proj2 = feature_source_projection(
            feature_group=mock_fg,
            features=["id", "age", "country"],
            where=[
                ("age", ">", 25),
                ("country", "in", ["US", "UK"])
            ]
        )
        print("✅ Multiple tuple filters work")
    except Exception as e:
        print(f"❌ Multiple tuple filters failed: {e}")
        return False
    
    return True


def test_dict_filters_removed():
    """Test that dictionary filters are no longer supported"""
    print("\nTesting that dictionary filters are removed...")
    
    class MockFeatureGroup:
        def __init__(self, name):
            self.name = name
            self.keys = ["id"]
            self.location = f"/mock/{name}"
        
        def exists(self):
            return True
    
    mock_fg = MockFeatureGroup("test")
    
    # Test single dictionary filter should fail
    try:
        proj1 = feature_source_projection(
            feature_group=mock_fg,
            features=["id", "status"],
            where={"column": "status", "operator": "==", "value": "ACTIVE"}
        )
        print("❌ Dictionary filter still works - should have been removed!")
        return False
    except (ValueError, TypeError) as e:
        print("✅ Dictionary filter correctly rejected")
    except Exception as e:
        print(f"❌ Dictionary filter failed with unexpected error: {e}")
        return False
    
    # Test list of dictionary filters should fail
    try:
        proj2 = feature_source_projection(
            feature_group=mock_fg,
            features=["id", "age"],
            where=[
                {"column": "age", "operator": ">", "value": 25}
            ]
        )
        print("❌ Dictionary filter list still works - should have been removed!")
        return False
    except (ValueError, TypeError) as e:
        print("✅ Dictionary filter list correctly rejected")
    except Exception as e:
        print(f"❌ Dictionary filter list failed with unexpected error: {e}")
        return False
    
    return True


def main():
    print("🧪 Testing Filter Format Changes")
    print("=" * 40)
    
    # Test that tuple filters still work
    tuple_ok = test_tuple_filters_work()
    
    # Test that dictionary filters are removed
    dict_removed = test_dict_filters_removed()
    
    print("\n📊 Test Results:")
    print(f"   Tuple filters work: {'✅' if tuple_ok else '❌'}")
    print(f"   Dictionary filters removed: {'✅' if dict_removed else '❌'}")
    
    if tuple_ok and dict_removed:
        print("\n🎉 All tests passed! Dictionary filter format successfully removed.")
        print("   Only tuple format is now supported: ('column', 'operator', 'value')")
        return True
    else:
        print("\n❌ Some tests failed. Please review the implementation.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)