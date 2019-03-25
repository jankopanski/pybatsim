from batsim.batsim import Job

from procset import ProcSet



class Dataset:

    def __init__(self, id, size):
        self._id = id                       # UID of the dataset
        self._size = size                   # Size in bytes of the dataset (float)
        self._timestamp = 0                 # When the dataset has been added to a Storage

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


class Storage:
    
    def __init__(self, id, name, storage_capacity):
        self._id = id                               # Resource id of the storage
        self._name = name                           # Name of the storage
        self._storage_capacity = storage_capacity   # Capacity of the storage in bytes (float)
        self._available_space = storage_capacity    # Current available space of the storage in bytes (float)
        self._datasets = dict()                     # Dict of dataset_id -> Dataset object

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
        """ Returns a Dataset corresponding to the dataset_id if it exists"""

        return self._datasets[dataset_id] if dataset_id in self._datasets else None

    def add_dataset(self, dataset, timestamp):
        """ Add Dataset to the Storage
        
        When adding a dataset, its timestamp is set to current time.
        Then, we subtract the available space on storage with the size of the Dataset.
        """

        dataset.set_timestamp(timestamp)
        self._datasets[dataset._id] = dataset
        self._available_space = self._available_space - dataset.get_size()

    def delete_dataset(self, dataset_key):
        """ Delete the Dataset on Storage corresponding to the dataset_key 
        
        When deleting a Dataset, its size is added to remaining storage space on Storage
        """

        self._available_space = self._available_space + self._datasets[dataset_key].get_size()
        self._datasets.pop(dataset_key)

    def has_enough_space(self, size):
        """ Returns true if the Storage has enough space to store a dataset corresponding to the
        provided size.
        """

        return self._available_space - size >= 0

class StorageController:

    def __init__(self, storage_resources, bs, qn):
        self._storages = dict()  # Maps the storage batsim id to the Storage object
        self._ceph_id = -1       # The batsim id of the storage_server
        self._idSub = 0
        self._bs = bs            # Pybatsim
        self._qn = qn            # The QNode Scheduler
        self._logger = bs.logger

        self.mappingQBoxes = {}  # Maps the disk index to the QBox Scheduler object
        self.moveRequested = {}  # Maps the job_id to the dataset_id

        for res in storage_resources:
            self.add_storage(Storage(res["id"], res["name"], float(res["properties"]["size"])))

            if res["name"] == "storage_server":
                self._ceph_id = res["id"]

        '''
        Clement: I think the filename holding all the datasets in a simulation will
        be directly passed as argument of the scheduler in pybatsim (and forwarded here).
        So yet another TODO:
        Need to read the list of the datasets (one JSON object per line corresponding to one dataset)
        that are assumed to be available in the CEPH at t=0 (since the CEPH has an 'infinite' storage capacity)

        In reality, the datasets are available at the submission time of the QTask that needs it,
        but for the case of Qarnot schedulers it does not change anything (I guess).
        '''

        self._logger.info("[{}]- StorageController initialization completed, CEPH id is {} and there are {} QBox disks".format(self._bs.time(),self._ceph_id, len(self._storages)-1))


    def get_storage(self, storage_id):
        """ Returns the Storage corresponding to given storage_id """

        return self._storages[storage_id]

    def get_storages_by_dataset(self, required_dataset):
        """ Returns the list of storages that already has the required dataset """

        qboxes_list = [] # The dict of all qboxes with the required dataset
        # To check the storages
        for storage in self.get_storages():
            # At this point, storage is an ID. Let's take the storage (Class) with this ID.
            storage = self.get_storage(storage)
            self._logger.info("{} storages: {} " .format(storage._name, storage._datasets))
            hasDataset = True
            # To check if this storage has all the datasets required.
            for dataset in required_dataset:
                if (storage.get_dataset(dataset) == None):
                    hasDataset = False
                    break
            # If true, the storage has all required datasets. So, candidate_qb = {storages}
            if(hasDataset):
                self._logger.info("     This QBOX has the required dataset. QBOX: ", self.mappingQBoxes[storage._id])
                qboxes_list.append(self.mappingQBoxes[storage._id])
        
        return qboxes_list

    def add_storage(self, storage):
        """ Add storage to storages list """

        self._storages[storage._id] = storage

    def add_dataset(self, storage_id, dataset):
        """ Add to the given storage the dataset """

        storage = self.get_storage(storage_id)

        assert storage.get_storage_capacity() >= dataset.get_size(), "The dataset %r is larger than the storage capacity, aborting." % dataset._id

        if not storage.has_enough_space(dataset.get_size()):
            self.clear_storage(storage, dataset)

        storage.add_dataset(dataset, self._bs.time())


    def move_to_dest(self, dataset_ids, dest_id):
        """ Method used to move datasets from the CEPH to the disk of a QBox """
        for dataset_id in dataset_ids:
            self.move_dataset(dataset_id, self._ceph_id, dest_id)



    def move_dataset(self, dataset_id, source_id, dest_id):
        """ Copy a Dataset from a source to a destination given source_id and dest_id respectively 
        
        If we can't move the Dataset, then no job for it is scheduled
        """

        #TODO check if a datasest already on the qbox disk no need to send it
        # TODO check if the move of a dataset already asked towards the same qbox disk, no need to send it twice

        source = self.get_storage(source_id)
        dest = self.get_storage(dest_id)
        dataset = source.get_dataset(dataset_id)

        storage = self.get_storage(source_id)

        assert dataset is not None, "Error: Dataset %r not found in source %r" % (dataset_id, source_id)

        assert dest.get_storage_capacity() >= dataset.get_size(), "The dataset %r is larger than the storage capacity, aborting." % dataset._id
        
        if dest.get_dataset(dataset_id) is not None:
            self._logger.info("Dataset already in dest")
            return

        # Clear storage to enable data transfer
        if not dest.has_enough_space(dataset.get_size()):
            self.clear_storage(dest, dataset)

        # Profile Submit
        profile_name = "staging" + str(self._idSub + 1)
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
        self._idSub += 1
        jid1 = "dyn-staging!" + str(self._idSub)
        self._bs.register_job(id=jid1, res=1, walltime=-1, profile_name=profile_name)

        # Job Execution
        job1 = Job(jid1, 0, -1, 1, "", "")
        job1.allocation = ProcSet(source_id, dest_id)
        job1.storage_mapping = {}
        job1.storage_mapping[source._name] = source_id
        job1.storage_mapping[dest._name] = dest_id
        self._bs.execute_jobs([job1])
        self.moveRequested[jid1] = dataset.get_id()

        self._logger.info("[", self._bs.time(), "] StorageController starting move dataset", dataset_id, "to qbox disk", dest_id)

    def clear_strategy(self, storage):
        """ LRU implementation to clean Storage of the oldest Dataset 
        
        This method can be changed to implement other caching strategy if needed.
        """
        
        return min(storage.get_datasets(), key=(lambda key: storage.get_datasets()[key].get_timestamp()))

    def clear_storage(self, storage, dataset):
        """ Clear the storage until it has enough space to store the given dataset """

        ## ATTENTION !!!!!!!
        # TODO
        # Ajouter des infos sur les datasets qui sont utilisés par des running jobs.
        # Pour ne pas les enlever du disque pendant que le job tourne encore ...

        while not storage.has_enough_space(dataset.get_size()):
            # Caching strategy call
            dataset_to_delete = self.clear_strategy(storage) 
            storage.delete_dataset(dataset_to_delete)


    ''' This function should be called during init of the QBox Scheduler '''
    def onQBoxRegistration(self, qbox_name, qbox):
        qbox_disk_name = qbox_name + "_disk"
        for disk in self._storages.values():
            if (disk._name == qbox_disk_name):
                #TODO we will also need king of the inverse mapping from the qbox_id to the storage
                self.mappingQBoxes[disk._id] = qbox
                return disk._id

        assert False, "QBox {} registered but no corresponding disk was found".format(qbox_name)


    def onQBoxAskHardLink(self, qb_disk_id, dataset_id):
        '''
        TODO
        This function is called from a QBox scheduler when new instances of a QTask starts running
        A hard link should be created for all the datasets that are inputs of the QTask
        '''
        pass

    def onQBoxReleaseHardLinks(self, qb_disk_id, dataset_ids):
        '''
        TODO
        This function is called from a QBox scheduler when there are no more
        instances of a QTask that uses their input datasets.
        Multiple hardlink are released
        '''
        pass

    def onQBoxAskDataset(self, qb_disk_id, dataset_id):
        '''
        TODO
        This function is called from a QBox scheduler and asks for a dataset to be on disk.
        If the dataset is already on disk, return True
        If not, start data staging job and return False
        WARNING! If the data staging of that dataset on this qbox disk was already asked, return False but don't start another data staging job.
        '''
        return True

# Handlers of Batsim-related events

    def onDataStagingCompletion(self, job):
        dest_id = list(job.allocation)[0] # TODO index 0 of the allocation should always be the machine id of a qbox disk and not the storage server, but should not be hardcodded like that...
        dataset_id = self.moveRequested.pop(job.id)

        # TODO make a HARD copy of the dataset when it's been downloaded on another storage
        dataset = self.get_storage(self._ceph_id).get_dataset(dataset_id)

        self.get_storage(dest_id).add_dataset(dataset, job.finish_time)

        self.mappingQBoxes[dest_id].onDatasetArrived(dataset_id)


    def onSimulationBegins(self):
        pass

    def onSimulationEnds(self):
        self._logger.info("End of simulation")
        for storage in self._storages.values():
            self._logger.info("{} contains the following Datasets:{}".format(storage._name,", ".join(storage._datasets.keys())))

    def onNotifyEventNewDatasetOnStorage(self, machines, dataset_id, dataset_size):
        for machine_id in machines:
            self.add_dataset(machine_id, Dataset(dataset_id, float(dataset_size)))
