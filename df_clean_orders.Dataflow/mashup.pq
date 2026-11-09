[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_orders_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "order_id", DestinationColumnName = "order_id"], [SourceColumnName = "customer_id", DestinationColumnName = "customer_id"], [SourceColumnName = "product_id", DestinationColumnName = "product_id"], [SourceColumnName = "city", DestinationColumnName = "city"], [SourceColumnName = "amount", DestinationColumnName = "amount"], [SourceColumnName = "order_date", DestinationColumnName = "order_date"], [SourceColumnName = "updated_at", DestinationColumnName = "updated_at"], [SourceColumnName = "amount_band", DestinationColumnName = "amount_band"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared raw_orders = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ebb37344-5499-4bce-91db-afa9ff20741e"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "325f957c-64fb-4c53-b231-fa31b084f050"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "raw_orders", ItemKind = "Table"]}[Data],
  #"Replaced value" = Table.ReplaceValue(#"Navigation 2", null, "Unknown", Replacer.ReplaceValue, {"city"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value", {{"amount", type number}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "amount_band", each if [amount] = null then "Unknown"
else if [amount] >= 3000 then "High"
else "Normal"), {{"amount_band", type text}})
in
  #"Added custom";
shared raw_orders_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "ebb37344-5499-4bce-91db-afa9ff20741e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "325f957c-64fb-4c53-b231-fa31b084f050"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "df_orders_clean", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
