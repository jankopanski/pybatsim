from batsim.storageController import StorageController, Dataset
from batsim.batsim import Job

import json

class QarnotStorageController(StorageController):
    def __init__(self, storage_resources, bs, sched, options):
        super().__init__(storage_resources, bs, sched, options)
        
        # Maps a Batsim index of storage to the QBox Scheduler object
        # Will be filled during call of onQBoxRegistration
        self.mappingQBoxes = {}

        main_storage = self.storages[self.main_storage_id]
        main_storage.qb_name = "main_storage"

        # Populate the main_storage with all datasets
        filename = self.options["input_path"] + "/datasets.json"
        with open(filename, 'r') as file:
            # Iterate over every line (which is a json object)
            for cnt, line in enumerate(file):
                parsed = json.loads(line)
                main_storage.add_dataset(Dataset(parsed["id"], parsed["size"]), 0)




    def data_staging_completed(self, job):
        # OVERRIDING the StorageController function
        data_transfer = self.current_transfers.pop(job.id)
        dataset = self.get_storage(data_transfer.source_id).get_dataset_from_id(data_transfer.dataset_id)
        dest = self.get_storage(data_transfer.dest_id)
        dest.reserved_space -= dataset.get_size()

        if job.job_state == Job.State.COMPLETED_SUCCESSFULLY:
            self.add_dataset_to_storage(data_transfer.dest_id, dataset, job.finish_time)
            #self.scheduler.onDatasetArrivedOnStorage(*data_transfer)
            self.mappingQBoxes[dest.id].onDatasetArrivedOnDisk(data_transfer.dataset_id)
        else:
            # The data_staging job was either killed or it failled
            if self.qn.end_of_simulation_asked:
                pass
            else:
                raise NotImplementedYet() # This should never happen


    def onQBoxAskHardLink(self, storage_id, dataset_id, job_id):
        '''
        This function is called when a QBox requests a hardlink for an input dataset of a given job.
        A hardlink should be created between this job_id and the dataset in the given storage.
        '''
        storage = self.get_storage(storage_id)

        assert storage != None, f"Requesting a hard link for {job_id} on a non-existent storage (id {storage_id})"
        assert storage.has_dataset(dataset_id), f"Requesting a hard link for {job_id} on dataset {dataset_id} that is not in storage {storage.get_name()} (id {storage_id})"

        storage.add_hard_link(dataset_id, job_id, self.bs.time())
        self.logger.debug(f"[{self.bs.time()}] StorageController added a hard link for {job_id} to {dataset_id} on storage id {storage_id}.")


    def onQBoxReleaseHardLink(self, storage_id, dataset_id, job_id):
        '''
        This function is called when a QBox requests the release of a specific hard link between
        a job and dataset on a given storage.
        '''
        storage = self.get_storage(storage_id)
        assert storage != None, f"Requesting the release of a hard link for {job_id} on a non-existent storage (id {storage_id})"
        assert storage.has_dataset(dataset_id), f"Requesting the release of a hard link for {job_id} on dataset {dataset_id} that is not in storage {storage.get_name()} (if {storage_id})"

        storage.remove_hard_link(dataset_id, job_id)


    def onQBoxReleaseAllHardLinks(self, storage_id, job_id):
        '''
        This function is called when all hard links to a given job should be released.
        '''
        storage = self.get_storage(storage_id)
        assert storage != None, f"Requesting the release of all hard links of {job_id} on a non-existent storage (id {storage_id})"
        
        storage.remove_all_hard_links(job_id)




    ''' This function should be called during init of the QBox Scheduler '''
    def onQBoxRegistration(self, qbox_name, qbox):
        qbox_disk_name = qbox_name + "_disk"
        for disk in self.storages.values():
            if (disk.name == qbox_disk_name):
                self.mappingQBoxes[disk.id] = qbox
                disk.qb_name = qbox.name
                return disk.id

        assert False, "QBox {} registered but no corresponding disk was found".format(qbox_name)


    def onQBoxAskDatasetOnStorage(self, storage_id, dataset_id):
        return self.ask_data_transfer(dataset_id, self.main_storage_id, storage_id)


    
    def onSimulationBegins(self):
        pass

    def onSimulationEnds(self):
        self.logger.info("End of simulation")
        for storage in self.storages.values():
            self.logger.info("{} is filled at {} / {}.".format(storage.name, (storage.storage_capacity-storage.available_space), storage.storage_capacity))

        #self.logger.info("Staging jobs collected : {}".format(self.traces.shape))
        #if self.output_path != None:
        #    self.traces.to_csv(self.output_path + '/staging_jobs.csv')
        return (self.nb_transfers_zero, self.nb_transfers_real, self.total_transferred_from_main)

    '''def onNotifyEventNewDatasetOnStorage(self, machines, dataset_id, dataset_size):
        for machine_id in machines:
            self.add_dataset(machine_id, Dataset(dataset_id, float(dataset_size)))
    '''





    def getNMostEmptyStorages(self, n=1):
        '''
        Gets the list of storages with the most empty space remaining.
        If the remaining space is same, sorted ascending with storage_id.

        n is truncated to the available number of qboxes if it is greater than it.
        :param n: Value of n
        :return: list of qbox ids
        '''
        # Maps the available space to storage_id
        size_map = dict()

        for batsim_id, storage in self.get_storages_dict().items():
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
            self.onQBoxAskDatasetOnStorage(storage_id, dataset_id)


    def replicateDatasetOnAllDisks(self, dataset_id):
        '''
        Simply asks to replicate the given dataset onto all disks.
        '''
        for storage_id in self.get_storages_dict().keys():
            self.onQBoxAskDatasetOnStorage(storage_id, dataset_id)


    def onGetStoragesHavingDatasets(self, datasets):
        ''' Returns the list of QBox names that already has the required datasets '''

        # The list of all qboxes with the required dataset
        qboxes_list = []

        # Iterate over all the storages
        for storage in self.get_storages_dict().values():
            # Check if this storage has all the required datasets.
            hasDataset = True
            for dataset_id in datasets:
                if not storage.has_dataset(dataset_id):
                    hasDataset = False
                    break
            if hasDataset:
                # If true, this storage has all the required datasets, add it to the list
                qboxes_list.append(storage.qb_name)

        self.logger.debug(f"[{self.bs.time()}] StorageController found the required datasets on: {qboxes_list}")

        return qboxes_list
