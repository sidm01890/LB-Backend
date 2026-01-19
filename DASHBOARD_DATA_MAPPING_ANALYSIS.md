# Dashboard Data Mapping Analysis: `devyani_posvszom` Collection to Frontend Widgets

## Overview
This document analyzes how to map data from the MongoDB collection `devyani_posvszom` to populate all dashboard widgets and graphs in the `/dashboard` frontend.

---

## 1. Dashboard Widgets & Their Data Requirements

### 1.1 Top-Level Cards (DashboardNumbers Component)
**Location:** `LB-Fronend/src/Pages/Pages/Dashboard/components/DashboardNumbers.jsx`

**Widgets:**
- **POS Sales Card** (when Aggregator + POS Sales selected)
- **AGGREGATOR Card** (when Aggregator + 3PO Sales selected)

**Data Source:** `dashboard3POData` from Redux
- `posSales` → POS Sales card
- `threePOSales` → AGGREGATOR card

---

### 1.2 Total Sales Chart
**Location:** `LB-Fronend/src/Pages/Pages/Dashboard/3PODashboardGraphs/Charts/TotalSales.jsx`

**Data Required:**
- **Array:** `dashboard3POData.threePOData[]` (for 3PO Sales view)
- **Array:** `dashboard3POData.tenderWisePOSData[]` (for POS Sales view)

**Each Array Item Needs:**
```javascript
{
  tenderName: "ZOMATO" | "SWIGGY",  // Required for labels
  posSales: number,                  // For POS Sales view
  threePOSales: number              // For 3PO Sales view
}
```

**Current Issue:** Arrays are empty `[]`, so chart shows nothing. Need to populate from collection.

---

### 1.3 Status Comparison Chart (POS vs 3PO)
**Location:** `LB-Fronend/src/Pages/Pages/Dashboard/3PODashboardGraphs/Charts/StatusComparison/`

**Data Required:**
- **Array:** `dashboard3POData.threePOData[]` or `dashboard3POData.tenderWisePOSData[]`
- **Selectable Options:** "POS vs 3PO", "Receivable vs Receipts", "Promo"

**Each Array Item Needs:**
```javascript
{
  tenderName: "ZOMATO" | "SWIGGY",
  posVsThreePO: number,              // For "POS vs 3PO" option
  receivablesVsReceipts: number,     // For "Receivable vs Receipts" option
  promo: number,                     // For "Promo" option
  threePOSales: number,              // Used as denominator for percentage calculation
  totalReceivables: number,           // For ReceivableVsReceiptsChart
  totalReceipts: number               // For ReceivableVsReceiptsChart
}
```

---

### 1.4 Receivables Chart
**Location:** `LB-Fronend/src/Pages/Pages/Dashboard/3PODashboardGraphs/Charts/Receivables/`

**Data Required:**
- **Array:** `dashboard3POData.threePOData[]` (for 3PO Sales)
- **Array:** `dashboard3POData.tenderWisePOSData[]` (for POS Sales)

**Each Array Item Needs:**
```javascript
{
  tenderName: "ZOMATO" | "SWIGGY",
  threePOReceivables: number,        // For 3PO Sales view
  posReceivables: number             // For POS Sales view
}
```

---

### 1.5 Reconciled Chart
**Location:** `LB-Fronend/src/Pages/Pages/Dashboard/3PODashboardGraphs/Charts/Reconciled3PO/`

**Data Required:**
- **Array:** `dashboard3POData.threePOData[]`

**Each Array Item Needs:**
```javascript
{
  tenderName: "ZOMATO" | "SWIGGY",
  reconciled: number                 // Count or amount of reconciled items
}
```

---

### 1.6 Charges Chart
**Location:** `LB-Fronend/src/Pages/Pages/Dashboard/3PODashboardGraphs/Charts/Charges3PO/`

**Data Required:**
- **Array:** `dashboard3POData.threePOData[]`
- **Selectable Options:** "All Charges", "Charges", "Promo", "Discounts", "Freebies", "Commission"

**Each Array Item Needs:**
```javascript
{
  tenderName: "ZOMATO" | "SWIGGY",
  // For 3PO Sales view:
  threePOCharges: number,
  threePOCommission: number,
  threePODiscounts: number,
  threePOFreebies: number,
  // For POS Sales view:
  posCharges: number,
  posCommission: number,
  posDiscounts: number,
  posFreebies: number,
  // Common:
  promo: number
}
```

**All Charges Calculation:**
- 3PO: `threePOCharges + promo + threePODiscounts + threePOFreebies + threePOCommission`
- POS: `posCharges + promo + posDiscounts + posFreebies + posCommission`

---

## 2. Current API Response Structure

**Endpoint:** `/threePODashboardData`

**Current Response:**
```json
{
  "success": true,
  "data": {
    "posSales": 54625.23,              // ✅ Populated (from pos_payment)
    "posReceivables": 0.0,             // ❌ Empty
    "posCommission": 0.0,              // ❌ Empty
    "posCharges": 0.0,                 // ❌ Empty
    "posDiscounts": 0,                 // ❌ Empty
    "threePOSales": 60003.17,          // ✅ Populated (from net_amount)
    "threePOReceivables": 0.0,         // ❌ Empty
    "threePOCommission": 0.0,          // ❌ Empty
    "threePOCharges": 0.0,             // ❌ Empty
    "threePODiscounts": 0,              // ❌ Empty
    "reconciled": 0,                    // ❌ Empty
    "receivablesVsReceipts": 0.0,      // ❌ Empty
    "posFreebies": 0,                   // ❌ Empty
    "threePOFreebies": 0,               // ❌ Empty
    "posVsThreePO": 0,                 // ❌ Empty
    "booked": 0,                        // ❌ Empty
    "promo": 0,                         // ❌ Empty
    "deltaPromo": 0,                    // ❌ Empty
    "allThreePOCharges": 0,            // ❌ Empty
    "allPOSCharges": 0,                 // ❌ Empty
    "threePOData": [],                  // ❌ EMPTY ARRAY - CRITICAL
    "tenderWisePOSData": [],            // ❌ EMPTY ARRAY - CRITICAL
    "instoreTotal": 0
  }
}
```

---

## 3. MongoDB Collection: `devyani_posvszom`

### 3.1 Assumed Collection Structure
Based on the naming convention and dashboard requirements, the collection likely contains calculated/aggregated data per tender (ZOMATO, SWIGGY, etc.).

**Expected Document Structure:**
```javascript
{
  _id: ObjectId,
  tender_name: "ZOMATO" | "SWIGGY",    // Grouping field
  order_date: ISODate,                  // For date filtering
  store_name: string,                   // For store filtering
  
  // POS Fields
  pos_payment: number,                  // Already used for posSales
  pos_receivables: number,
  pos_commission: number,
  pos_charges: number,
  pos_discounts: number,
  pos_freebies: number,
  
  // 3PO/Aggregator Fields
  net_amount: number,                   // Already used for threePOSales
  three_po_receivables: number,
  three_po_commission: number,
  three_po_charges: number,
  three_po_discounts: number,
  three_po_freebies: number,
  
  // Comparison Fields
  pos_vs_three_po: number,
  receivables_vs_receipts: number,
  
  // Other Fields
  reconciled: number,
  promo: number,
  total_receivables: number,
  total_receipts: number,
  booked: number,
  delta_promo: number,
  
  // Calculated totals
  all_three_po_charges: number,
  all_pos_charges: number
}
```

**Note:** The actual field names may differ. You need to verify the exact field names in your MongoDB collection.

---

## 4. Data Mapping Strategy

### 4.1 Aggregation Pipeline Design

**Step 1: Filter by Date Range and Stores**
```javascript
{
  $match: {
    order_date: {
      $gte: start_datetime,
      $lte: end_datetime
    },
    store_name: { $in: stores }  // If store filtering is needed
  }
}
```

**Step 2: Group by Tender Name**
```javascript
{
  $group: {
    _id: "$tender_name",  // or "$tenderName" - verify field name
    
    // POS Aggregations
    posSales: { $sum: { $ifNull: ["$pos_payment", 0] } },
    posReceivables: { $sum: { $ifNull: ["$pos_receivables", 0] } },
    posCommission: { $sum: { $ifNull: ["$pos_commission", 0] } },
    posCharges: { $sum: { $ifNull: ["$pos_charges", 0] } },
    posDiscounts: { $sum: { $ifNull: ["$pos_discounts", 0] } },
    posFreebies: { $sum: { $ifNull: ["$pos_freebies", 0] } },
    
    // 3PO Aggregations
    threePOSales: { $sum: { $ifNull: ["$net_amount", 0] } },
    threePOReceivables: { $sum: { $ifNull: ["$three_po_receivables", 0] } },
    threePOCommission: { $sum: { $ifNull: ["$three_po_commission", 0] } },
    threePOCharges: { $sum: { $ifNull: ["$three_po_charges", 0] } },
    threePODiscounts: { $sum: { $ifNull: ["$three_po_discounts", 0] } },
    threePOFreebies: { $sum: { $ifNull: ["$three_po_freebies", 0] } },
    
    // Comparison Fields
    posVsThreePO: { $sum: { $ifNull: ["$pos_vs_three_po", 0] } },
    receivablesVsReceipts: { $sum: { $ifNull: ["$receivables_vs_receipts", 0] } },
    
    // Other Fields
    reconciled: { $sum: { $ifNull: ["$reconciled", 0] } },
    promo: { $sum: { $ifNull: ["$promo", 0] } },
    totalReceivables: { $sum: { $ifNull: ["$total_receivables", 0] } },
    totalReceipts: { $sum: { $ifNull: ["$total_receipts", 0] } },
    booked: { $sum: { $ifNull: ["$booked", 0] } },
    deltaPromo: { $sum: { $ifNull: ["$delta_promo", 0] } }
  }
}
```

**Step 3: Project and Format**
```javascript
{
  $project: {
    tenderName: "$_id",
    posSales: 1,
    posReceivables: 1,
    posCommission: 1,
    posCharges: 1,
    posDiscounts: 1,
    posFreebies: 1,
    threePOSales: 1,
    threePOReceivables: 1,
    threePOCommission: 1,
    threePOCharges: 1,
    threePODiscounts: 1,
    threePOFreebies: 1,
    posVsThreePO: 1,
    receivablesVsReceipts: 1,
    reconciled: 1,
    promo: 1,
    totalReceivables: 1,
    totalReceipts: 1,
    booked: 1,
    deltaPromo: 1,
    allThreePOCharges: {
      $add: [
        "$threePOCharges",
        "$promo",
        "$threePODiscounts",
        "$threePOFreebies",
        "$threePOCommission"
      ]
    },
    allPOSCharges: {
      $add: [
        "$posCharges",
        "$promo",
        "$posDiscounts",
        "$posFreebies",
        "$posCommission"
      ]
    }
  }
}
```

---

## 5. Implementation Plan

### 5.1 Backend Changes (`/threePODashboardData` API)

**Current Logic:**
- ✅ Gets report names from `formulas` collection
- ✅ Aggregates `pos_payment` and `net_amount` from report collections
- ❌ Returns empty arrays for `threePOData` and `tenderWisePOSData`

**New Logic Needed:**

1. **Query `devyani_posvszom` collection:**
   ```python
   collection = get_mongodb_collection("devyani_posvszom")
   
   pipeline = [
       {
           "$match": {
               "order_date": {
                   "$gte": start_datetime,
                   "$lte": end_datetime
               }
               # Add store filter if needed
           }
       },
       {
           "$group": {
               "_id": "$tender_name",  # Verify field name
               # ... all aggregations as shown above
           }
       },
       {
           "$project": {
               # ... format as shown above
           }
       }
   ]
   
   tender_wise_data = list(collection.aggregate(pipeline))
   ```

2. **Transform to API Response Format:**
   ```python
   # Calculate top-level totals
   total_pos_sales = sum(item.get("posSales", 0) for item in tender_wise_data)
   total_three_po_sales = sum(item.get("threePOSales", 0) for item in tender_wise_data)
   
   # Build arrays
   three_po_data = []
   tender_wise_pos_data = []
   
   for item in tender_wise_data:
       # Both arrays get the same data, but frontend uses different ones based on salesType
       tender_item = {
           "tenderName": item.get("tenderName", ""),
           "posSales": float(item.get("posSales", 0)),
           "posReceivables": float(item.get("posReceivables", 0)),
           "posCommission": float(item.get("posCommission", 0)),
           "posCharges": float(item.get("posCharges", 0)),
           "posDiscounts": int(item.get("posDiscounts", 0)),
           "threePOSales": float(item.get("threePOSales", 0)),
           "threePOReceivables": float(item.get("threePOReceivables", 0)),
           "threePOCommission": float(item.get("threePOCommission", 0)),
           "threePOCharges": float(item.get("threePOCharges", 0)),
           "threePODiscounts": int(item.get("threePODiscounts", 0)),
           "reconciled": int(item.get("reconciled", 0)),
           "receivablesVsReceipts": float(item.get("receivablesVsReceipts", 0)),
           "posFreebies": int(item.get("posFreebies", 0)),
           "threePOFreebies": int(item.get("threePOFreebies", 0)),
           "posVsThreePO": float(item.get("posVsThreePO", 0)),
           "booked": int(item.get("booked", 0)),
           "promo": int(item.get("promo", 0)),
           "deltaPromo": int(item.get("deltaPromo", 0)),
           "allThreePOCharges": float(item.get("allThreePOCharges", 0)),
           "allPOSCharges": float(item.get("allPOSCharges", 0)),
           "totalReceivables": float(item.get("totalReceivables", 0)),
           "totalReceipts": int(item.get("totalReceipts", 0))
       }
       
       three_po_data.append(tender_item)
       tender_wise_pos_data.append(tender_item)
   
   # Build response
   response = {
       "posSales": total_pos_sales,
       "posReceivables": sum(item.get("posReceivables", 0) for item in tender_wise_data),
       "posCommission": sum(item.get("posCommission", 0) for item in tender_wise_data),
       "posCharges": sum(item.get("posCharges", 0) for item in tender_wise_data),
       "posDiscounts": sum(item.get("posDiscounts", 0) for item in tender_wise_data),
       "threePOSales": total_three_po_sales,
       "threePOReceivables": sum(item.get("threePOReceivables", 0) for item in tender_wise_data),
       "threePOCommission": sum(item.get("threePOCommission", 0) for item in tender_wise_data),
       "threePOCharges": sum(item.get("threePOCharges", 0) for item in tender_wise_data),
       "threePODiscounts": sum(item.get("threePODiscounts", 0) for item in tender_wise_data),
       "reconciled": sum(item.get("reconciled", 0) for item in tender_wise_data),
       "receivablesVsReceipts": sum(item.get("receivablesVsReceipts", 0) for item in tender_wise_data),
       "posFreebies": sum(item.get("posFreebies", 0) for item in tender_wise_data),
       "threePOFreebies": sum(item.get("threePOFreebies", 0) for item in tender_wise_data),
       "posVsThreePO": sum(item.get("posVsThreePO", 0) for item in tender_wise_data),
       "booked": sum(item.get("booked", 0) for item in tender_wise_data),
       "promo": sum(item.get("promo", 0) for item in tender_wise_data),
       "deltaPromo": sum(item.get("deltaPromo", 0) for item in tender_wise_data),
       "allThreePOCharges": sum(item.get("allThreePOCharges", 0) for item in tender_wise_data),
       "allPOSCharges": sum(item.get("allPOSCharges", 0) for item in tender_wise_data),
       "threePOData": three_po_data,              # ✅ NOW POPULATED
       "tenderWisePOSData": tender_wise_pos_data, # ✅ NOW POPULATED
       "instoreTotal": total_pos_sales
   }
   ```

---

## 6. Field Name Mapping Reference

### 6.1 Critical Fields to Verify in MongoDB Collection

**Grouping Field:**
- `tender_name` or `tenderName` or `tender` → Used for `tenderName` in response

**Date Field:**
- `order_date` or `orderDate` → Used for date filtering

**POS Fields (verify exact names):**
- `pos_payment` / `posPayment` → `posSales`
- `pos_receivables` / `posReceivables` → `posReceivables`
- `pos_commission` / `posCommission` → `posCommission`
- `pos_charges` / `posCharges` → `posCharges`
- `pos_discounts` / `posDiscounts` → `posDiscounts`
- `pos_freebies` / `posFreebies` → `posFreebies`

**3PO Fields (verify exact names):**
- `net_amount` / `netAmount` → `threePOSales`
- `three_po_receivables` / `threePOReceivables` → `threePOReceivables`
- `three_po_commission` / `threePOCommission` → `threePOCommission`
- `three_po_charges` / `threePOCharges` → `threePOCharges`
- `three_po_discounts` / `threePODiscounts` → `threePODiscounts`
- `three_po_freebies` / `threePOFreebies` → `threePOFreebies`

**Comparison Fields:**
- `pos_vs_three_po` / `posVsThreePO` → `posVsThreePO`
- `receivables_vs_receipts` / `receivablesVsReceipts` → `receivablesVsReceipts`

**Other Fields:**
- `reconciled` → `reconciled`
- `promo` → `promo`
- `total_receivables` / `totalReceivables` → `totalReceivables`
- `total_receipts` / `totalReceipts` → `totalReceipts`
- `booked` → `booked`
- `delta_promo` / `deltaPromo` → `deltaPromo`

---

## 7. Testing Checklist

After implementation, verify:

- [ ] Top-level `posSales` and `threePOSales` are populated
- [ ] `threePOData` array has items with `tenderName` field
- [ ] `tenderWisePOSData` array has items with `tenderName` field
- [ ] Total Sales chart displays bars for each tender
- [ ] Status Comparison chart works for all 3 options
- [ ] Receivables chart displays data
- [ ] Reconciled chart displays data
- [ ] Charges chart displays data for all 6 options
- [ ] All numeric fields are properly formatted (float/int)
- [ ] Date filtering works correctly
- [ ] Store filtering works (if applicable)

---

## 8. Next Steps

1. **Verify Collection Structure:**
   - Connect to MongoDB and inspect `devyani_posvszom` collection
   - Check actual field names
   - Check data types
   - Verify if data is already aggregated or needs aggregation

2. **Update Backend API:**
   - Modify `/threePODashboardData` endpoint
   - Add aggregation pipeline for `devyani_posvszom` collection
   - Map fields correctly
   - Populate `threePOData` and `tenderWisePOSData` arrays

3. **Test Frontend:**
   - Verify all charts populate correctly
   - Check data formatting
   - Test with different date ranges
   - Test with different salesType selections

---

## 9. Important Notes

1. **Field Name Variations:** MongoDB field names might use snake_case (`pos_payment`) or camelCase (`posPayment`). Verify and adjust accordingly.

2. **Data Types:** Ensure numeric fields are properly converted to float/int as expected by frontend.

3. **Empty Collections:** Handle cases where collection is empty or has no matching documents gracefully.

4. **Multiple Tenders:** The aggregation should handle multiple tenders (ZOMATO, SWIGGY, etc.) and return separate items for each.

5. **Date Format:** Ensure `order_date` field in MongoDB matches the date format used in filtering.

---

**End of Analysis Document**



