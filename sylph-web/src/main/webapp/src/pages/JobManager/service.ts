import { request } from 'umi';

export async function queryJobs() {
  return request('/_sys/job_manger/jobs');
}