import { PeerInfo } from '@/components/PeerInfo';
import ReloadButton from '@/components/ReloadButton';
import { PeerSlotResponse, PeerStatResponse } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import LagGraph from './lagGraph';
import SlotTable from './slottable';
import StatTable from './stattable';

type DataConfigProps = {
  params: { peerName: string };
};

const PeerData = async ({ params: { peerName } }: DataConfigProps) => {
  const getSlotData = async () => {
    try{
    const flowServiceAddr = GetFlowHttpAddressFromEnv();

    const peerSlots: PeerSlotResponse = await fetch(
      `${flowServiceAddr}/v1/peers/slots/${peerName}`,
      {
        cache: 'no-store',
      }
    ).then((res) => res.json())
    .catch((e) => {
      console.error('Error fetching slots:', e);
      return [];
    });

    const slotArray = peerSlots.slotData??[];
    // slots with 'peerflow_slot' should come first
    slotArray?.sort((slotA, slotB) => {
      if (
        slotA.slotName.startsWith('peerflow_slot') &&
        !slotB.slotName.startsWith('peerflow_slot')
      ) {
        return -1;
      } else if (
        !slotA.slotName.startsWith('peerflow_slot') &&
        slotB.slotName.startsWith('peerflow_slot')
      ) {
        return 1;
      } else {
        return 0;
      }
    });
    return slotArray;
  } catch (e) {
    console.error('Error fetching slots:', e);
    return [];
  }
  };

  const getStatData = async () => {
    try{
    const flowServiceAddr = GetFlowHttpAddressFromEnv();

    const peerStats: PeerStatResponse = await fetch(
      `${flowServiceAddr}/v1/peers/stats/${peerName}`,
      { cache: 'no-store' }
    ).then((res) => res.json())
    .catch((e) => {
      console.error('Error fetching stats:', e);
      return [];
    });

    return peerStats.statData ?? [];
    } catch (e) {
      console.error('Error fetching stats:', e);
      return [];
    };
  }

  const slots = await getSlotData();
  const stats = await getStatData();

  return (
    <div
      style={{
        padding: '2rem',
        width: '100%',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <div
          style={{
            fontSize: 20,
            fontWeight: 'bold',
            display: 'flex',
            alignItems: 'center',
            columnGap: '1rem',
          }}
        >
          <div>{peerName}</div>
          <PeerInfo peerName={peerName} />
        </div>
        <ReloadButton />
      </div>

      {slots && stats ? (
        <div>
          <SlotTable data={slots} />
          <LagGraph slotNames={slots.map((slot) => slot.slotName)} />
          <StatTable data={stats} />
        </div>
      ) : (
        <div>
          <Label
            as='label'
            style={{ fontSize: 18, marginTop: '1rem', display: 'block' }}
          >
            Peer Statistics
          </Label>
          <Label as='label' style={{ fontSize: 15, marginTop: '1rem' }}>
            No stats to show
          </Label>
        </div>
      )}
    </div>
  );
};

export default PeerData;
