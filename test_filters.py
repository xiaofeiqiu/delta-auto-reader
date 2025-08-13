#!/usr/bin/env python3
"""
Test script to verify that only ConditionTuple format is supported.
"""
import sys
sys.path.append('.')

from feature_store_sdk.projection import FeatureSourceProjection, feature_source_projection
from feature_store_sdk import FeatureStore, BatchFeatureGroup, c


def test_condition_tuple_filters_work():
    """Test that ConditionTuple filters work"""
    print("Testing ConditionTuple filter format...")
    
    # Mock feature group for testing
    class MockFeatureGroup:
        def __init__(self, name):
            self.name = name
            self.keys = ["id"]
            self.location = f"/mock/{name}"
        
        def exists(self):
            return True
    
    mock_fg = MockFeatureGroup("test")
    
    # Test single ConditionTuple filter
    try:
        proj1 = feature_source_projection(
            feature_group=mock_fg,
            features=["id", "status"],
            where=[c("status", "==", "ACTIVE")]
        )
        print("✅ Single ConditionTuple filter works")
    except Exception as e:
        print(f"❌ Single ConditionTuple filter failed: {e}")
        return False
    
    # Test multiple ConditionTuple filters
    try:
        proj2 = feature_source_projection(
            feature_group=mock_fg,
            features=["id", "age", "country"],
            where=[
                c("age", ">", 25),
                c("country", "in", ["US", "UK"])
            ]
        )
        print("✅ Multiple ConditionTuple filters work")
    except Exception as e:
        print(f"❌ Multiple ConditionTuple filters failed: {e}")
        return False
    
    return True


def test_old_formats_removed():
    """Test that old filter formats are no longer supported"""
    print("\nTesting that old filter formats are removed...")
    
    class MockFeatureGroup:
        def __init__(self, name):
            self.name = name
            self.keys = ["id"]
            self.location = f"/mock/{name}"
        
        def exists(self):
            return True
    
    mock_fg = MockFeatureGroup("test")
    
    # Test single tuple filter should fail
    try:
        proj1 = feature_source_projection(
            feature_group=mock_fg,
            features=["id", "status"],
            where=("status", "==", "ACTIVE")  # Regular tuple, not ConditionTuple
        )
        print("❌ Regular tuple filter still works - should have been removed!")
        return False
    except (ValueError, TypeError) as e:
        print("✅ Regular tuple filter correctly rejected")
    except Exception as e:
        print(f"❌ Regular tuple filter failed with unexpected error: {e}")
        return False
    
    # Test list of regular tuples should fail
    try:
        proj2 = feature_source_projection(
            feature_group=mock_fg,
            features=["id", "age"],
            where=[
                ("age", ">", 25)  # Regular tuple, not ConditionTuple
            ]
        )
        print("❌ Regular tuple list still works - should have been removed!")
        return False
    except (ValueError, TypeError) as e:
        print("✅ Regular tuple list correctly rejected")
    except Exception as e:
        print(f"❌ Regular tuple list failed with unexpected error: {e}")
        return False
    
    return True


def main():
    print("🧪 Testing Filter Format Changes")
    print("=" * 40)
    
    # Test that ConditionTuple filters work
    condition_tuple_ok = test_condition_tuple_filters_work()
    
    # Test that old formats are removed
    old_removed = test_old_formats_removed()
    
    print("\n📊 Test Results:")
    print(f"   ConditionTuple filters work: {'✅' if condition_tuple_ok else '❌'}")
    print(f"   Old formats removed: {'✅' if old_removed else '❌'}")
    
    if condition_tuple_ok and old_removed:
        print("\n🎉 All tests passed! Only ConditionTuple format is supported.")
        print("   Use c() helper: [c('column', 'operator', 'value')]")
        return True
    else:
        print("\n❌ Some tests failed. Please review the implementation.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)