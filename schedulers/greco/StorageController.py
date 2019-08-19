from batsim.batsim import Job

from procset import ProcSet
import pandas as pd

import xml.etree.ElementTree as ET

import math
import json

class Dataset:

    def __init__(self, id, size, timestamp = 0):
        self._id = id                       # UID of the dataset
        self._size = size                   # Size in bytes of the dataset (float)
        self._timestamp = timestamp         # When the dataset has been added to a Storage
        self._running_job = set()           # The id of the running jobs that are using this Dataset

    def get_id(self):
        return self._id

    def get_size(self):
        """ Returns the size of the Dataset """
        return self._size
    
    def get_timestamp(self):
        """ Returns the timestamp of the Dataset """
        return self._timestamp
    
    def set_timestamp(self, timestamp):
        """ Update the timestamp of the Dataset """
        self._timestamp = timestamp

    def add_running_job(self, job_id):
        """ Append the id of the job that is using this dataset """
        self._running_job.add(job_id)

    def delete_running_job(self, job_id):
        """ Deletes the job_id from the list
        Returns True if the job id was present in the set
        Else returns false """
        if job_id in self._running_job:
            self._running_job.remove(job_id)
            return True
        else:
            return False

    def get_running_jobs(self):
        """ Gets the list of running job ids """
        ret = []

        for val in self._running_job:
            ret.append(val)

        return ret

    def is_job_running(self):
        """ See if there is any running job present """
        if(len(self._running_job) != 0):
            return True
        else:
            return False


class Storage:
    
    def __init__(self, id, name, storage_capacity):
        self._id = id                                   # Resource id of the storage
        self._name = name                               # Name of the storage
                                                        # Assumption : If name is "storage_server", it means it is ceph

        if(self._name == "storage_server"):
            self.is_ceph = True
            self._storage_capacity = 1e15           # Capacity set to 1000TB (should be enough) (float)
            self._available_space = 1e15            # Available space set to 1000TB (float)
        else:
            self.is_ceph = False
            self._storage_capacity = storage_capacity   # Capacity of the storage in bytes (float)
            self._available_space = storage_capacity    # Current available space of the storage in bytes (float)

        self._datasets = dict()                         # Dict of dataset_id -> Dataset object

    def load_datasets_from_json(self, filename):
        # Open the dataset file
        with open(filename, 'r') as file:

            # Iterate over every line (which is a json object)
            for cnt, line in enumerate(file):

                # Parse this object
                parsed = json.loads(line)

                # Store this object in the dataset with timestamp 0
                self.add_dataset(Dataset(parsed["id"], parsed["size"]), 0)

    def get_id(self):
        return self._id

    def get_available_space(self):
        """ Returns the remaining space on the Storage """

        return self._available_space

    def get_storage_capacity(self):
        """ Returns the storage cpacity of the Storage """

        return self._storage_capacity

    def get_datasets(self):
        """ Returns the list of datasets on the Storage """

        return self._datasets

    def get_dataset(self, dataset_id):
        """ Returns a Dataset corresponding to the dataset_id if it exists """

        return self._datasets[dataset_id] if dataset_id in self._datasets else None


    def add_dataset(self, dataset, timestamp):
        """ Add Dataset to the Storage
        If the dataset is already present, then it updates the timestamp
        
        When adding a dataset, its timestamp is set to current time.
        Then, we subtract the available space on storage with the size of the Dataset only if the data was not present.
        """

        # Check if the storage already has the dataset with this id. if yes, do not subtract available space
        if(self.get_dataset(dataset.get_id()) == None):
            self._available_space = self._available_space - dataset.get_size()

        # Then update the timestamp of last use
        dataset.set_timestamp(timestamp)
        self._datasets[dataset._id] = dataset


    def delete_dataset(self, dataset_id):
        """ Delete the Dataset on Storage corresponding to the dataset_key 
        
        When deleting a Dataset, its size is added to remaining storage space on Storage.
        Returns False if the Dataset was not present in this Storage.
        Returns True if the Dataset was correctly removed.
        """
        dataset = self.get_dataset(dataset_id)

        # Check if dataset is not present
        if dataset == None:
            return False

        self._available_space = self._available_space + dataset.get_size()
        self._datasets.pop(dataset_id)

        return True

    def has_dataset(self, dataset_id):
        '''

        :param dataset_id: The id of the dataset
        :return: True if this storage has the storage of that id
        '''
        dataset = self.get_dataset(dataset_id)

        if(dataset == None):
            return False
        else:
            return True

    def has_enough_space(self, size):
        """ Returns true if the Storage has enough space to store a dataset corresponding to the
        provided size.
        """
        return (self._available_space - size) >= 0

    def update_timestamp(self, dataset_id, timestamp):
        '''

        :param dataset_id: Datasetid to update timestamp with
        :return: False if dataset with id not present, True else
        '''
        if(self.get_dataset(dataset_id) == None):
            return False

        dataset = self.get_dataset(dataset_id)

        self.add_dataset(dataset, timestamp)

    def releaseHardLinks(self, qtask_id):
        '''
        Releases hardlinks to a given qtask_id

        :param qtask_id: The qtask id of the task
        :return: True
        '''

        for dataset in self.get_datasets().values():
            dataset.delete_running_job(qtask_id)

        return True


class StorageController:

    def __init__(self, storage_resources, bs, qn, options):
        self._input_path = options["input_path"]
        if "output_path" in options:
            self._output_path = options["output_path"]
        else:
            self._output_path = None

        self._traces = pd.DataFrame(columns=['count', 'name', 'dataset_id', 'source', 'dest',
                                             'actually_transferred_size','transfer_size',
                                             'direction', 'status'])
        self._count = 0
        self._storages = dict()  # Maps the storage batsim id to the Storage object
        self._ceph_id = -1       # The batsim id of the storage_server
        self._next_staging_job_id = 0
        self._bs = bs            # Pybatsim
        self._qn = qn            # The QNode Scheduler
        self._logger = bs.logger
        self._total_transferred_from_CEPH = 0 # In Bytes
        self._nb_transfers_zero = 0 # The number of times a dataset was already present on disk
        self._nb_transfers_real = 0 # The number of "real" transfers

        # Stores the requests that have been staged.
        # If transfer from CEPH to storage_id i fro dataset_id j
        # Then this will have the entry (i, j)
        self.staging_map = set()

        self.mappingQBoxes = {}  # Maps the disk index to the QBox Scheduler object
        self.moveRequested = {}  # Maps the job_id to the dataset_id

        for res in storage_resources:
            new_storage = Storage(res["id"], res["name"], float(res["properties"]["size"]))

            # If it is the CEPH Server
            if res["name"] == "storage_server":

                # Store the CEPH ID
                self._ceph_id = res["id"]
                # Parse the dataset file
                new_storage.load_datasets_from_json(self._input_path+"/datasets.json")

            self.add_storage(new_storage)

        self._logger.info("[{}]- StorageController initialization completed, CEPH id is {} and there are {} QBox disks".format(self._bs.time(),self._ceph_id, len(self._storages)-1))

        # Now parse the platform file to get the bandwidth values

        tree = ET.parse(self._input_path+'/platform.xml')

        self.bandwidth = {}

        for child in tree.getroot().iter('link'):
            if ('wan_link' in child.attrib['id']) and ('bandwidth' in child.attrib):
                self.bandwidth[child.attrib['id'][:-9]] = float(child.attrib['bandwidth'][:-4])

    def get_storage(self, storage_id):
        """ Returns the Storage corresponding to given storage_id if it exists or returns None. """
        return self._storages[storage_id] if storage_id in self._storages else None

    def get_storages(self):
        """ Returns the Storages in the Storage Controller """
        return self._storages

    def add_storage(self, storage):
        """ Add storage to storages list """
        self._storages[storage._id] = storage


    def add_dataset(self, storage_id, dataset, timestamp=0):
        """ Add to the given storage the dataset

        Returns False if the storage does not exist
        Else return True
        Asserts if the capacity is greater than required
        timestamp of the dataset is updated if exists.
        """
        storage = self.get_storage(storage_id)

        if(storage == None):
            return False

        # Check if the storage already has the dataset
        if(storage.get_dataset(dataset.get_id()) != None):
            self._logger.debug("[{}] Dataset {} already present in storage with id {}".format(self._bs.time(), dataset.get_id(), storage_id))

            storage.update_timestamp(dataset.get_id(), timestamp)

            return True

        # Now check if the dataset fits in the storage
        assert storage.get_storage_capacity() >= dataset.get_size(), "The dataset %r is larger than the storage capacity, aborting." % dataset._id

        if not storage.has_enough_space(dataset.get_size()):
            self.clear_storage(storage, dataset)

        storage.add_dataset(dataset, self._bs.time())
        return True

    def has_dataset(self, storage_id, dataset_id):
        '''
        Check if a storage has a dataset

        :param storage_id: Id of the storage to find in
        :param dataset_id: Id of the dataset to find
        :return: True if it exists, else false
        '''
        storage = self.get_storage(storage_id)

        if(storage == None):
            return False
        else:
            return storage.has_dataset(dataset_id)

    def get_free_bandwidth_between_storages(self, source_id, dest_id):
        '''
        Gets a rough estimate of the free bandwidth (in bits/sec) to move from one storage to other.
        Uses staged jobs to find which of the network links are congested.
        If source_id is not CEPH, then dest_id is replaced by CEPH
        TODO: Update this function to enable qbox to qbox transfers.

        :param source_id: Source of the transfer
        :param dest_id: Destination of the transfer
        :return: Cost of free bandwidth assuming equal share
        '''

        if source_id == dest_id:
            return 1000000000       # 1Tbps

        id = source_id
        if source_id == self._ceph_id:
            id = dest_id

        speed = 1000.0        # 1Gbps

        if dest_id in self.bandwidth:
            speed = self.bandwidth[dest_id]

        transfers = 0
        for val in self.staging_map:
            if val[0] == id:
                transfers = transfers + 1
            elif val[1] == id:
                transfers = transfers + 1

        transfers = transfers + 1   # Assuming the current one starts
        return float(speed)/float(transfers)


    def copy_from_CEPH_to_dest(self, dataset_ids, dest_id):
        """ Method used to move datasets from the CEPH to the disk of a QBox

        A false return indicates that one of the inserts had an issue.
        See the documentation of the copy_dataset function
        """
        status = True
        
        for dataset_id in dataset_ids:
            temp = self.copy_dataset(dataset_id, self._ceph_id, dest_id)
            status = (status & temp)

        return status


    def copy_dataset(self, dataset_id, source_id, dest_id):
        """
        COPY a Dataset from a source to a destination given source_id and dest_id respectively

        Returns true iff:
        1. The dataset is already present in the destination.
        2. If not present in destination, but, source exists, destination exists, dataset exists in source and move is possible.

        Returns false iff:
        1. Dataset not present in source and destination, also destination is different than source.
        2. Source or destination id do not exist

        If we can't move the Dataset, then no job for it is scheduled
        """
        self._logger.debug("StorageController : Request for dataset {} to transfer from {} to {}"\
                          .format(dataset_id, source_id, dest_id))

        source = self.get_storage(source_id)
        dest = self.get_storage(dest_id)
        dataset = source.get_dataset(dataset_id)

        self._count = self._count + 1

        entry = {'count': self._count, 'name' : "", 'dataset_id': dataset_id, 'source': source_id, 'dest' : dest_id,
                'actually_transferred_size' : 0, 'transfer_size' : dataset.get_size(),
                'direction' : 'Down', 'status' : 'default'}

        # First check if destination exists
        if(dest == None):
            entry['status'] = 'null_dest'
            self._traces = self._traces.append(entry, ignore_index=True)
            self._logger.info("StorageController : Destination storage with id {} not found".format(dest_id))
            return False

        # Now we know that destination is present and does not have the dataset

        # We check if the source exists
        if(source == None):
            entry['status'] = 'null_source'
            self._traces = self._traces.append(entry, ignore_index=True)
            self._logger.info("StorageController : Source storage with id {} not found".format(dest_id))
            return False

        # Now check if the source has the dataset required
        if(source.get_dataset(dataset_id) == None):
            entry['status'] = 'data_absent_source'
            self._traces = self._traces.append(entry, ignore_index=True)
            self._logger.info("StorageController : Source with id {} does not have dataset with id {}.".format(source_id, dest_id))
            return False

        # Now check if destination has dataset
        if(dest.get_dataset(dataset_id) != None):
            entry['status'] = 'data_present_dest'
            self._traces = self._traces.append(entry, ignore_index=True)
            self._logger.debug("StorageController : Dataset with id {} already present in destination with id {}.".format(dataset_id, dest_id))
            self._nb_transfers_zero+=1
            return True

        # Now we check if the destination has enough storage

        assert dest.get_storage_capacity() >= dataset.get_size(), \
            "StorageController : The dataset %r is larger than the storage capacity, aborting." % dataset._id
        
        # Clear storage to enable data transfer
        if not dest.has_enough_space(dataset.get_size()):
            entry['status'] = 'insufficient_space'
            self.clear_storage(dest, dataset)

        # Profile Submit
        profile_name = "staging" + str(self._next_staging_job_id + 1)
        move_profile = {
            profile_name : 
            {
                'type' : 'data_staging', 
                'nb_bytes' : dataset.get_size(), 
                'from' : source._name, 
                'to' : dest._name
            },
        }
        self._bs.register_profiles("dyn-staging", move_profile)

        # Job Submit
        self._next_staging_job_id += 1
        jid1 = "dyn-staging!" + str(self._next_staging_job_id)
        self._bs.register_job(id=jid1, res=1, walltime=-1, profile_name=profile_name)

        # Job Execution
        job1 = Job(jid1, 0, -1, 1, "", "")
        job1.allocation = ProcSet(source_id, dest_id)
        job1.storage_mapping = {}
        job1.storage_mapping[source._name] = source_id
        job1.storage_mapping[dest._name] = dest_id
        self._bs.execute_jobs([job1])
        self.moveRequested[jid1] = dataset.get_id()

        self._logger.info("[ {} ] Storage Controller staging job for dataset {} from {} to {} started".format(self._bs.time(), dataset_id, source_id, dest_id))

        self._nb_transfers_real+=1
        self._total_transferred_from_CEPH+=dataset.get_size()

        self.staging_map.add((dest_id, dataset_id))

        entry['actually_transferred_size'] = dataset.get_size()
        self._traces = self._traces.append(entry, ignore_index=True)
        return False


    def clear_strategy(self, storage):
        """ LRU implementation to clean Storage of the oldest Dataset
        This method can be changed to implement other caching strategy if needed.

        Returns the id of the dataset which is not used by any job and has the minimum value of timestamp (LRU)
        Returns -1 if it is not possible to remove any dataset.
        """
        # This is used so that we can initialize the fields the first time a valid one is encountered.
        dataset_final = -1
        min_valid_timestamp = None

        for dataset_id, dataset in storage.get_datasets().items():

            # This is to find the first valid dataset
            if dataset_final == -1:
                if not (dataset.is_job_running()):
                    dataset_final = dataset_id
                    min_valid_timestamp = dataset.get_timestamp()
            # If the first one is found, then check the parameters
            elif dataset.is_job_running():
                continue
            elif min_valid_timestamp > dataset.get_timestamp():
                dataset_final = dataset_id
                min_valid_timestamp = dataset.get_timestamp()

        return dataset_final


    def clear_storage(self, storage, dataset):
        """ Clear the storage until it has enough space to store the given dataset
        Breaks if not possible to remove dataset
        """
        while not storage.has_enough_space(dataset.get_size()):
            # Caching strategy call
            dataset_to_delete = self.clear_strategy(storage)
            if dataset_to_delete != -1:
                storage.delete_dataset(dataset_to_delete)
            else:
                break

    def getNMostEmptyStorages(self, n=1):
        '''
        Gets the list of storages with the most empty space remaining.
        If the remaining space is same, sorted ascending with storage_id.

        n is truncated to the available number of qboxes if it is greater than it.
        :param n: Value of n
        :return: list of qbox ids
        '''

        # Maps the empty size to disk_id
        size_map = dict()

        for batsim_id, storage in self.get_storages().items():
            size_map[storage.get_id()] = storage.get_available_space()

        # Now sort
        sorted_list = sorted(size_map, key=size_map.__getitem__)
        sorted_list.reverse()

        # Check if requested number is greater than the qboxes, if yes, truncate
        ret_len = min(n, len(sorted_list))
        return sorted_list[0:ret_len]


    def replicateDatasetOnStorages(self, dataset_id, storage_ids):
        ''' Replicate the Dataset on given storages '''
        for storage_id in storage_ids:
            self.onQBoxAskDataset(storage_id, dataset_id)


    def replicateDatasetOnAllDisks(self, dataset_id):
        '''
        Used by the FullReplicate version.
        Simply asks to replicate the given dataset onto all QBox disks.
        '''
        for storage_id in self.get_storages().keys():
            self.onQBoxAskDataset(storage_id, dataset_id)


    def onGetStoragesHavingDatasets(self, datasets):
        """ Returns the list of storages (without CEPH) that already has the required datasets """

        # The list of all qboxes with the required dataset
        qboxes_list = []

        # Iterate over all the storages
        for storage in self.get_storages().values():
            # Check if this storage has all the required datasets.
            hasDataset = True
            for dataset_id in datasets:
                if storage.has_dataset(dataset_id):
                    hasDataset = False
                    break
            if hasDataset:
                # If true, this storage has all the required datasets, add it to the list
                qboxes_list.append(storage._qb_name)

        return qboxes_list


    ''' This function should be called during init of the QBox Scheduler '''
    def onQBoxRegistration(self, qbox_name, qbox):
        qbox_disk_name = qbox_name + "_disk"
        for disk in self._storages.values():
            if (disk._name == qbox_disk_name):
                #TODO we will also need kind of the inverse mapping from the qbox_id to the storage
                self.mappingQBoxes[disk._id] = qbox
                disk._qb_name = qbox.name
                return disk._id

        assert False, "QBox {} registered but no corresponding disk was found".format(qbox_name)


    def onQBoxAskHardLink(self, storage_id, dataset_id, qtask_id):
        '''
        This function is called when a QBox requests a hardlink for an input dataset of a given qtask.
        A hardlink should be created between this QTask and the dataset.

        Returns True if the hard link was added.
        '''
        storage = self.get_storage(storage_id)
        if storage is None :
            assert False, "Requesting a hard link on a non-existent storage {}".format(storage_id)

        dataset = storage.get_dataset(dataset_id)
        if dataset is None:
            assert False, "Requestion a hard link on dataset {} that is not in the disk {}".format(dataset_id, storage_id)

        dataset.add_running_job(qtask_id)
        self._logger.debug("[{}] StorageController added a hard link to {} for {} on disk {}.".format(self._bs.time(), dataset_id, qtask_id, storage_id))

        return True


    def onQBoxReleaseHardLinks(self, storage_id, qtask_id):
        '''
        This function is called when all instances of a QTask have finished in a QBox.
        The hardlinks of all datasets for this QTask should be released.

        Return True if the hard link was released.
        '''

        storage = self.get_storage(storage_id)
        if storage is None :
            assert False, "Releasing a hard link on a non-existent storage {}".format(storage_id)

        storage.releaseHardLinks(qtask_id)


    def onQBoxAskDataset(self, storage_id, dataset_id):
        '''
        This function is called from a QBox scheduler and asks for a dataset to be on disk.
        
        Only returns true if the dataset is already present in the Qbox.
        If the data staging of that dataset on this qbox disk was already asked, returns False but doesnt start
        another data staging job.
        '''
    
        if((storage_id, dataset_id) in self.staging_map):
            self._nb_transfers_zero+=1
            return False
        # Else add the dataset
        else:
            return self.copy_from_CEPH_to_dest([dataset_id], storage_id)

    def onKillAllStagingJobs(self):
        # This is called by the QNode scheduler upon receiving a 'stop_simulation' external event
        # return the list of data staging jobs to be killed in order to finish the simulation
        self._logger.info("[{}] StorageController returns {} data staging jobs to be killed".format(self._bs.time(), len(self.moveRequested.keys())))
        return self.moveRequested.keys()
        

# Handlers of Batsim-related events

    def onDataStagingCompletion(self, job):
        dest_id = list(job.allocation)[0]
        # TODO index 0 of the allocation should always be the machine id of a qbox disk and not the storage server,
        #  but should not be hardcodded like that...

        dataset_id = self.moveRequested.pop(job.id)

        dataset = self.get_storage(self._ceph_id).get_dataset(dataset_id)

        # Create a hard copy
        dataset_new = Dataset(dataset.get_id(), dataset.get_size())

        self.get_storage(dest_id).add_dataset(dataset_new, job.finish_time)

        # Remove from the staging knowledge
        self.staging_map.remove((dest_id, dataset_id))

        self.mappingQBoxes[dest_id].onDatasetArrivedOnDisk(dataset_id)

    def onSimulationBegins(self):
        pass

    def onSimulationEnds(self):
        self._logger.info("End of simulation")
        for storage in self._storages.values():
            #self._logger.info("{} contains the following Datasets:{}".format(storage._name,", ".join(storage._datasets.keys())))
            self._logger.info("{} is filled at {} / {}.".format(storage._name, (storage._storage_capacity-storage._available_space), storage._storage_capacity))

        self._logger.info("Staging jobs collected : {}".format(self._traces.shape))
        if self._output_path != None:
            self._traces.to_csv(self._output_path + '/staging_jobs.csv')
        return (self._nb_transfers_zero, self._nb_transfers_real, self._total_transferred_from_CEPH)

    def onNotifyEventNewDatasetOnStorage(self, machines, dataset_id, dataset_size):
        for machine_id in machines:
            self.add_dataset(machine_id, Dataset(dataset_id, float(dataset_size)))\
