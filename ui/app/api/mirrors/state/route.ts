import {
  MirrorStatusRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body: MirrorStatusRequest = await request.json();
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const res: MirrorStatusResponse = await fetch(
    `${flowServiceAddr}/v1/mirrors/${body.flowJobName}`,
    { cache: 'no-store' }
  ).then((res) => res.json());

  return new Response(JSON.stringify(res));
}
