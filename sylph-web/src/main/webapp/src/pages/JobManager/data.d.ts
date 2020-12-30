export interface TableListItem {
  "jobName": string;
  "queryText": string;
  "type": string;
  "config": string;
  "description": string;
  "jobId": string;
  "status": string;
  "runId": string;
  "appUrl": string;
}

export interface TableListPagination {
  total: number;
  pageSize: number;
  current: number;
}

export interface TableListData {
  list: TableListItem[];
  pagination: Partial<TableListPagination>;
}

export interface TableListParams {
  status?: string;
  name?: string;
  desc?: string;
  key?: number;
  pageSize?: number;
  currentPage?: number;
  filter?: { [key: string]: any[] };
  sorter?: { [key: string]: any };
}